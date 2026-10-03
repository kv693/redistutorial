// Topic 3c: how a server "parks" a client on BRPOP. This is a toy model of
// what Redis does internally, not a Redis client.
//
// What matters:
//   - ONE goroutine (loop) owns all state and executes all commands, like
//     Redis's main thread, so there are no locks.
//   - Connection goroutines only read bytes and hand them to the loop. They
//     stand in for epoll/kqueue.
//   - "Parking" is bookkeeping, not a sleeping thread: the client is flagged
//     blocked and added to a per-key wait queue. The loop carries on serving
//     everyone else.
//   - A later LPUSH checks the wait queue and wakes the first parked client.
//   - A timer entry wakes a parked client with a nil reply if nothing arrives.
//
// Real Redis names, for when you read the source: c->flags & CLIENT_BLOCKED,
// db->blocking_keys, blockForKeys(), handleClientsBlockedOnKeys(), and a
// timeout table checked in the event loop.
//
// Protocol (one command per line): PING | LPUSH key val | BRPOP key seconds
package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------- server ---

type client struct {
	conn     net.Conn
	inbox    []string  // received but not yet executed lines
	blocked  bool      // the "parked" flag
	key      string    // the key we are parked on
	deadline time.Time // zero means wait forever, like BRPOP key 0
}

type event struct {
	c      *client
	line   string
	closed bool
}

type server struct {
	events    chan event
	lists     map[string][]string  // index 0 = head, last = tail
	blockedOn map[string][]*client // key -> FIFO queue of parked clients
	blocked   map[*client]struct{} // every parked client, for timeout scans
}

func newServer() *server {
	return &server{
		events:    make(chan event),
		lists:     map[string][]string{},
		blockedOn: map[string][]*client{},
		blocked:   map[*client]struct{}{},
	}
}

// accept stands in for the epoll "readable" notifications: every connection
// gets a goroutine that only turns bytes into events for the loop.
func (s *server) accept(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		c := &client{conn: conn}
		go func() {
			sc := bufio.NewScanner(conn)
			for sc.Scan() {
				s.events <- event{c: c, line: sc.Text()}
			}
			s.events <- event{c: c, closed: true}
		}()
	}
}

// loop is the "main thread". It is the only code that touches server state.
func (s *server) loop() {
	for {
		// Like epoll_wait with a timeout: wake for the next event OR the
		// nearest blocking deadline, whichever comes first.
		var timer <-chan time.Time
		if d, ok := s.nextDeadline(); ok {
			timer = time.After(time.Until(d))
		}

		select {
		case ev := <-s.events:
			if ev.closed {
				s.unpark(ev.c)
				continue
			}
			ev.c.inbox = append(ev.c.inbox, ev.line)
			s.process(ev.c)
		case <-timer:
			s.expireBlocked()
		}
	}
}

// process runs queued commands for one client, but stops while it is parked.
// Lines that arrive during the wait stay in the inbox, like bytes left in a
// blocked client's query buffer, and run after it wakes.
func (s *server) process(c *client) {
	for !c.blocked && len(c.inbox) > 0 {
		line := c.inbox[0]
		c.inbox = c.inbox[1:]
		s.execute(c, line)
	}
}

func (s *server) execute(c *client, line string) {
	f := strings.Fields(line)
	if len(f) == 0 {
		return
	}
	switch strings.ToUpper(f[0]) {
	case "PING":
		reply(c, "PONG")

	case "LPUSH":
		s.lists[f[1]] = append([]string{f[2]}, s.lists[f[1]]...)
		reply(c, fmt.Sprintf("(integer) %d", len(s.lists[f[1]])))
		s.serveBlocked(f[1]) // a push may satisfy someone who is parked

	case "BRPOP":
		key := f[1]
		secs, _ := strconv.Atoi(f[2])
		if v, ok := s.popTail(key); ok { // data is there: answer immediately
			reply(c, key+" "+v)
			return
		}
		s.park(c, key, secs) // no data: no reply now, flag and register
	}
}

// park is the heart of blocking: no thread sleeps, nothing is waited on.
// We set a flag, add the client to the key's wait queue, remember the
// deadline, and return so the loop moves on to the next event.
func (s *server) park(c *client, key string, secs int) {
	c.blocked, c.key = true, key
	if secs > 0 {
		c.deadline = time.Now().Add(time.Duration(secs) * time.Second)
	}
	s.blockedOn[key] = append(s.blockedOn[key], c)
	s.blocked[c] = struct{}{}
}

// serveBlocked runs right after a write command. While the list has data and
// someone is parked on it, hand elements to parked clients in arrival order
// (FIFO fairness). Redis does this in handleClientsBlockedOnKeys.
func (s *server) serveBlocked(key string) {
	for len(s.lists[key]) > 0 && len(s.blockedOn[key]) > 0 {
		c := s.blockedOn[key][0]
		v, _ := s.popTail(key)
		s.unpark(c)
		reply(c, key+" "+v) // the reply the client has been waiting for
		s.process(c)        // resume any commands queued while it was parked
	}
}

// expireBlocked wakes clients whose deadline has passed with a nil reply.
func (s *server) expireBlocked() {
	now := time.Now()
	for c := range s.blocked {
		if !c.deadline.IsZero() && !now.Before(c.deadline) {
			s.unpark(c)
			reply(c, "(nil)")
			s.process(c)
		}
	}
}

func (s *server) nextDeadline() (time.Time, bool) {
	var next time.Time
	for c := range s.blocked {
		if !c.deadline.IsZero() && (next.IsZero() || c.deadline.Before(next)) {
			next = c.deadline
		}
	}
	return next, !next.IsZero()
}

// unpark removes the client from the wait queue and clears the flag.
func (s *server) unpark(c *client) {
	if !c.blocked {
		return
	}
	q := s.blockedOn[c.key]
	for i, x := range q {
		if x == c {
			q = append(q[:i], q[i+1:]...)
			break
		}
	}
	if len(q) == 0 {
		delete(s.blockedOn, c.key)
	} else {
		s.blockedOn[c.key] = q
	}
	delete(s.blocked, c)
	c.blocked, c.deadline = false, time.Time{}
}

func (s *server) popTail(key string) (string, bool) {
	l := s.lists[key]
	if len(l) == 0 {
		return "", false
	}
	s.lists[key] = l[:len(l)-1]
	return l[len(l)-1], true
}

func reply(c *client, msg string) { fmt.Fprintln(c.conn, msg) }

// ------------------------------------------------------------------ demo ---

var start = time.Now()

func logf(format string, a ...any) {
	fmt.Printf("[%5dms] %s\n", time.Since(start).Milliseconds(), fmt.Sprintf(format, a...))
}

type conn struct {
	c net.Conn
	r *bufio.Reader
}

func dial(addr string) *conn {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	return &conn{c: c, r: bufio.NewReader(c)}
}

func (t *conn) send(line string) { fmt.Fprintln(t.c, line) }
func (t *conn) recv() string {
	s, _ := t.r.ReadString('\n')
	return strings.TrimSpace(s)
}

func main() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	s := newServer()
	go s.accept(ln)
	go s.loop()
	addr := ln.Addr().String()
	pusher := dial(addr)

	fmt.Println("== 1. A parks on an empty list; the server stays responsive ==")
	var wg sync.WaitGroup
	a, b := dial(addr), dial(addr)
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.send("BRPOP jobs 5")
		logf("A: sent BRPOP, now parked")
		logf("A: woke with %q", a.recv())
	}()
	time.Sleep(200 * time.Millisecond)
	b.send("PING")
	logf("B: PING -> %s   (A is parked, yet the server answered)", b.recv())
	time.Sleep(800 * time.Millisecond)
	logf("pusher: LPUSH jobs email-1")
	pusher.send("LPUSH jobs email-1")
	pusher.recv()
	wg.Wait()

	fmt.Println("\n== 2. Two parked clients are served in arrival order ==")
	d, e := dial(addr), dial(addr)
	wg.Add(1)
	go func() { defer wg.Done(); d.send("BRPOP q 5"); logf("D: woke with %q", d.recv()) }()
	time.Sleep(200 * time.Millisecond) // D is parked before E
	wg.Add(1)
	go func() { defer wg.Done(); e.send("BRPOP q 5"); logf("E: woke with %q", e.recv()) }()
	time.Sleep(200 * time.Millisecond)
	pusher.send("LPUSH q first")
	pusher.recv()
	time.Sleep(200 * time.Millisecond)
	pusher.send("LPUSH q second")
	pusher.recv()
	wg.Wait()

	fmt.Println("\n== 3. Commands sent while parked wait in the inbox ==")
	g := dial(addr)
	g.send("BRPOP slow 5")
	g.send("PING") // sent immediately, but must not run yet
	logf("G: sent BRPOP and PING back to back")
	time.Sleep(500 * time.Millisecond)
	pusher.send("LPUSH slow x")
	pusher.recv()
	logf("G: 1st reply %q", g.recv())
	logf("G: 2nd reply %q   (PING ran only after the wake-up)", g.recv())

	fmt.Println("\n== 4. Timeout wakes the client with nil ==")
	f := dial(addr)
	f.send("BRPOP nothing 1")
	logf("F: sent BRPOP nothing 1")
	logf("F: woke with %q", f.recv())
}
