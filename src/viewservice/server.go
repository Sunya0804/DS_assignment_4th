package viewservice

import (
	"net"
	"sync/atomic"
)
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     bool  // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	pingTimeMap          map[string]time.Time //  keeps track of most recent time VS heard ping from each server
	viewNum              map[string]uint      //  server id: viewNum
	currView             View                 //  keeps track of current view
	primaryAckedCurrView bool                 //  keeps track of whether primary has ACKed the current view
	nextViewNum          uint                 //  the next server's viewNum
	idleServer           [2]string            //  keeps track of any idle servers(next primary, next backup)
}

// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.primaryAckedCurrView == false {
		if (vs.currView.Primary == args.Me || vs.idleServer[0] == args.Me) &&
			vs.viewNum[vs.currView.Primary] == vs.currView.Viewnum {
			vs.primaryAckedCurrView = true
		} else {
			if vs.viewNum[vs.currView.Primary] == 0 && vs.currView.Viewnum == args.Viewnum { // pri srv first run
				vs.primaryAckedCurrView = true
			}
		}
	}

	vs.pingTimeMap[args.Me] = time.Now()
	vs.viewNum[args.Me] = args.Viewnum
	reply.View = vs.currView
	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	reply.View = vs.currView
	return nil
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// primary server가 없다면, 다음 primary server를 준비한다.
	if vs.currView.Primary == "" && vs.currView.Viewnum == vs.nextViewNum {
		nextPrimaryServer := ""
		for server, pingtime := range vs.pingTimeMap {
			if time.Now().Sub(pingtime) < DeadLine && server != "" {
				nextPrimaryServer = server
				break
			}
		}
		if nextPrimaryServer != "" {
			vs.idleServer[0] = nextPrimaryServer
			vs.nextViewNum = vs.currView.Viewnum + 1
			vs.primaryAckedCurrView = false
		}
	}

	// 백업 서버가 없다면, 다음 백업 서버를 준비한다.
	if vs.currView.Backup == "" && vs.currView.Viewnum == vs.nextViewNum {
		nextBackupServer := ""
		for server, pingtime := range vs.pingTimeMap {
			if time.Now().Sub(pingtime) < DeadLine && server != "" &&
				server != vs.currView.Primary {
				nextBackupServer = server
				break
			}
		}
		if nextBackupServer != "" {
			vs.idleServer[1] = nextBackupServer
			vs.nextViewNum = vs.currView.Viewnum + 1
			vs.primaryAckedCurrView = false
		}
	}

	// primary 서버가 죽었거나, restart 되었는지 확인
	primaryDead, backupDead := false, false
	if vs.currView.Primary != "" &&
		(time.Now().Sub(vs.pingTimeMap[vs.currView.Primary]) > DeadLine || // primary server가 죽은 경우
			vs.viewNum[vs.currView.Primary] == 0) { // primary server가 restart 된 경우
		primaryDead = true
	}

	// backup 서버가 죽었거나, restart 되었는지 확인
	if vs.currView.Backup != "" &&
		(time.Now().Sub(vs.pingTimeMap[vs.currView.Backup]) > DeadLine || // backup server가 죽은 경우
			vs.viewNum[vs.currView.Backup] == 0) { // backup server가 restart 된 경우
		backupDead = true
	}

	// primary server는 죽었지만, backup server가 살아있는 경우, backup server를 primary server로 변경한다.
	if primaryDead && vs.currView.Viewnum == vs.nextViewNum {
		if vs.currView.Backup != "" {
			vs.idleServer[0] = vs.currView.Backup
			vs.nextViewNum = vs.currView.Viewnum + 1
			vs.primaryAckedCurrView = false
		}
	}

	// backup server가 죽고 view number가 업데이트 되지 않은 경우, backup server를 없앤다.
	if backupDead && vs.currView.Viewnum == vs.nextViewNum {
		vs.currView.Backup = ""                  // backup server를 없앤다.
		vs.nextViewNum = vs.currView.Viewnum + 1 // 다음 view number를 준비한다.
		vs.primaryAckedCurrView = false          // primary server가 다음 view를 ACK하지 않은 상태로 변경한다.
	}

	// primary server가 마지막 view를 ACK하고, nextviewnum이 업데이트된 경우 새로운 view를 변경한다.
	if vs.primaryAckedCurrView == true && vs.nextViewNum != vs.currView.Viewnum {
		if vs.idleServer[0] != vs.currView.Primary && vs.idleServer[0] != "" {
			vs.currView.Primary = vs.idleServer[0]
			vs.idleServer[0] = ""
		}
		if vs.idleServer[1] != vs.currView.Backup && vs.idleServer[1] != "" {
			vs.currView.Backup = vs.idleServer[1]
			vs.idleServer[1] = ""
		}

		if vs.currView.Primary == vs.currView.Backup {
			vs.currView.Backup = ""
		}
		vs.currView.Viewnum = vs.nextViewNum
	}
}

// tell the server to shut itself down.
// for testing.
// please don't change this function.
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

// has this server been asked to shut down?
func (vs *ViewServer) isdead() bool {
	return vs.dead
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currView = View{Viewnum: 0, Primary: "", Backup: ""}
	vs.pingTimeMap = map[string]time.Time{}
	vs.primaryAckedCurrView = false
	vs.viewNum = map[string]uint{}
	vs.nextViewNum = 0

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
