/**
 * Copyright (c) 2014-2015, GoBelieve
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package main

import (
	"net"
	"sync"
	"time"

	log "github.com/golang/glog"
)

// Subscriber .
type Subscriber struct {
	uIDs    map[int64]int
	roomIDs map[int64]int
}

// NewSubscriber .
func NewSubscriber() *Subscriber {
	s := new(Subscriber)
	s.uIDs = make(map[int64]int)
	s.roomIDs = make(map[int64]int)
	return s
}

// Channel .
type Channel struct {
	addr string
	wt   chan *Message

	mutex       sync.Mutex
	subscribers map[int64]*Subscriber

	dispatch      func(*AppMessage)
	dispatchGroup func(*AppMessage)
	dispatchRoom  func(*AppMessage)
}

func NewChannel(addr string, f func(*AppMessage),
	f2 func(*AppMessage), f3 func(*AppMessage)) *Channel {
	channel := new(Channel)
	channel.subscribers = make(map[int64]*Subscriber)
	channel.dispatch = f
	channel.dispatchGroup = f2
	channel.dispatchRoom = f3
	channel.addr = addr
	channel.wt = make(chan *Message, 10)
	return channel
}

//返回添加前的计数
func (channel *Channel) AddSubscribe(appID, uid int64, online bool) (int, int) {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appID]
	if !ok {
		subscriber = NewSubscriber()
		channel.subscribers[appID] = subscriber
	}
	//不存在时count==0
	count := subscriber.uIDs[uid]

	//低16位表示总数量 高16位表示online的数量
	c1 := count & 0xffff
	c2 := count >> 16 & 0xffff

	if online {
		c2 += 1
	}
	c1 += 1
	subscriber.uIDs[uid] = c2<<16 | c1
	return count & 0xffff, count >> 16 & 0xffff
}

//返回删除前的计数
func (channel *Channel) RemoveSubscribe(appID, uid int64, online bool) (int, int) {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appID]
	if !ok {
		return 0, 0
	}

	count, ok := subscriber.uIDs[uid]
	//低16位表示总数量 高16位表示online的数量	
	c1 := count & 0xffff
	c2 := count >> 16 & 0xffff
	if ok {
		if online {
			c2 -= 1
			//assert c2 >= 0
			if c2 < 0 {
				log.Warning("online count < 0")
			}
		}
		c1 -= 1
		if c1 > 0 {
			subscriber.uIDs[uid] = c2<<16 | c1
		} else {
			delete(subscriber.uIDs, uid)
		}
	}
	return count & 0xffff, count >> 16 & 0xffff
}

func (channel *Channel) GetAllSubscriber() map[int64]*Subscriber {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	subs := make(map[int64]*Subscriber)
	for appid, s := range channel.subscribers {
		sub := NewSubscriber()
		for uid, c := range s.uIDs {
			sub.uIDs[uid] = c
		}

		subs[appid] = sub
	}
	return subs
}

//online表示用户不再接受推送通知(apns, gcm)
func (channel *Channel) Subscribe(appID int64, uid int64, online bool) {
	count, onlineCount := channel.AddSubscribe(appID, uid, online)
	log.Info("sub count:", count, onlineCount)
	if count == 0 {
		//新用户上线
		on := 0
		if online {
			on = 1
		}
		id := &SubscribeMessage{appid: appID, uid: uid, online: int8(on)}
		msg := &Message{cmd: MSG_SUBSCRIBE, body: id}
		channel.wt <- msg
	} else if onlineCount == 0 && online {
		//手机端上线
		id := &SubscribeMessage{appid: appID, uid: uid, online: 1}
		msg := &Message{cmd: MSG_SUBSCRIBE, body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) Unsubscribe(appID int64, uid int64, online bool) {
	count, onlineCount := channel.RemoveSubscribe(appID, uid, online)
	log.Info("unsub count:", count, onlineCount)
	if count == 1 {
		//用户断开全部连接
		id := &AppUserID{appid: appID, uid: uid}
		msg := &Message{cmd: MSG_UNSUBSCRIBE, body: id}
		channel.wt <- msg
	} else if count > 1 && onlineCount == 1 && online {
		//手机端断开连接,pc/web端还未断开连接
		id := &SubscribeMessage{appid: appID, uid: uid, online: 0}
		msg := &Message{cmd: MSG_SUBSCRIBE, body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) Publish(amsg *AppMessage) {
	msg := &Message{cmd: MSG_PUBLISH, body: amsg}
	channel.wt <- msg
}

func (channel *Channel) PublishGroup(aMsg *AppMessage) {
	msg := &Message{cmd: MSG_PUBLISH_GROUP, body: aMsg}
	channel.wt <- msg
}

func (channel *Channel) Push(appID int64, receivers []int64, msg *Message) {
	p := &BatchPushMessage{appid: appID, receivers: receivers, msg: msg}
	m := &Message{cmd: MSG_PUSH, body: p}
	channel.wt <- m
}

//返回添加前的计数
func (channel *Channel) AddSubscribeRoom(appID, roomID int64) int {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appID]
	if !ok {
		subscriber = NewSubscriber()
		channel.subscribers[appID] = subscriber
	}
	//不存在count==0
	count := subscriber.roomIDs[roomID]
	subscriber.roomIDs[roomID] = count + 1
	return count
}

//返回删除前的计数
func (channel *Channel) RemoveSubscribeRoom(appID, roomID int64) int {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appID]
	if !ok {
		return 0
	}

	count, ok := subscriber.roomIDs[roomID]
	if ok {
		if count > 1 {
			subscriber.roomIDs[roomID] = count - 1
		} else {
			delete(subscriber.roomIDs, roomID)
		}
	}
	return count
}

func (channel *Channel) GetAllRoomSubscriber() []*AppRoomID {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	subs := make([]*AppRoomID, 0, 100)
	for appID, s := range channel.subscribers {
		for roomID := range s.roomIDs {
			id := &AppRoomID{appid: appID, room_id: roomID}
			subs = append(subs, id)
		}
	}
	return subs
}

func (channel *Channel) SubscribeRoom(appID int64, roomID int64) {
	count := channel.AddSubscribeRoom(appID, roomID)
	log.Info("sub room count:", count)
	if count == 0 {
		id := &AppRoomID{appid: appID, room_id: roomID}
		msg := &Message{cmd: MSG_SUBSCRIBE_ROOM, body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) UnsubscribeRoom(appid int64, room_id int64) {
	count := channel.RemoveSubscribeRoom(appid, room_id)
	log.Info("unsub room count:", count)
	if count == 1 {
		id := &AppRoomID{appid: appid, room_id: room_id}
		msg := &Message{cmd: MSG_UNSUBSCRIBE_ROOM, body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) PublishRoom(amsg *AppMessage) {
	msg := &Message{cmd: MSG_PUBLISH_ROOM, body: amsg}
	channel.wt <- msg
}

func (channel *Channel) ReSubscribe(conn *net.TCPConn, seq int) int {
	subs := channel.GetAllSubscriber()
	for appid, sub := range (subs) {
		for uid, count := range (sub.uIDs) {
			//低16位表示总数量 高16位表示online的数量
			c2 := count >> 16 & 0xffff
			on := 0
			if c2 > 0 {
				on = 1
			}

			id := &SubscribeMessage{appid: appid, uid: uid, online: int8(on)}
			msg := &Message{cmd: MSG_SUBSCRIBE, body: id}

			seq = seq + 1
			msg.seq = seq
			SendMessage(conn, msg)
		}
	}
	return seq
}

func (channel *Channel) ReSubscribeRoom(conn *net.TCPConn, seq int) int {
	subs := channel.GetAllRoomSubscriber()
	for _, id := range (subs) {
		msg := &Message{cmd: MSG_SUBSCRIBE_ROOM, body: id}
		seq = seq + 1
		msg.seq = seq
		SendMessage(conn, msg)
	}
	return seq
}

func (channel *Channel) RunOnce(conn *net.TCPConn) {
	defer conn.Close()

	closedCh := make(chan bool)
	seq := 0
	seq = channel.ReSubscribe(conn, seq)
	seq = channel.ReSubscribeRoom(conn, seq)

	go func() {
		for {
			msg := ReceiveMessage(conn)
			if msg == nil {
				close(closedCh)
				return
			}
			log.Info("channel recv message:", Command(msg.cmd))
			if msg.cmd == MSG_PUBLISH {
				amsg := msg.body.(*AppMessage)
				if channel.dispatch != nil {
					channel.dispatch(amsg)
				}
			} else if msg.cmd == MSG_PUBLISH_ROOM {
				amsg := msg.body.(*AppMessage)
				if channel.dispatchRoom != nil {
					channel.dispatchRoom(amsg)
				}
			} else if msg.cmd == MSG_PUBLISH_GROUP {
				amsg := msg.body.(*AppMessage)
				if channel.dispatchGroup != nil {
					channel.dispatchGroup(amsg)
				}
			} else {
				log.Error("unknown message cmd:", msg.cmd)
			}
		}
	}()

	for {
		select {
		case _ = <-closedCh:
			log.Info("channel closed")
			return
		case msg := <-channel.wt:
			seq = seq + 1
			msg.seq = seq
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err := SendMessage(conn, msg)
			if err != nil {
				log.Info("channel send message:", err)
			}
		}
	}
}

func (channel *Channel) Run() {
	nsleep := 100
	for {
		conn, err := net.Dial("tcp", channel.addr)
		if err != nil {
			log.Info("connect route server error:", err)
			nsleep *= 2
			if nsleep > 60*1000 {
				nsleep = 60 * 1000
			}
			log.Info("channel sleep:", nsleep)
			time.Sleep(time.Duration(nsleep) * time.Millisecond)
			continue
		}
		tconn := conn.(*net.TCPConn)
		tconn.SetKeepAlive(true)
		tconn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
		log.Info("channel connected")
		nsleep = 100
		channel.RunOnce(tconn)
	}
}

func (channel *Channel) Start() {
	go channel.Run()
}
