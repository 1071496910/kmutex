package kmutex

import (
	"fmt"
	"sync"
)

type KMutex interface {
	Lock(key interface{})
	UnLock(key interface{})
}

var empty struct{}

//map+cond的实现，缺点容易引起goroutine的惊群现象，key比较少，等待key的routine比较多时，可能会有性能问题
type kMutex struct {
	l      *sync.Mutex //使用指针是因为mutex不能复制，否则起不到锁的效果
	cond   *sync.Cond
	mtxMap map[interface{}]struct{} //使用空struct大小为0节约空间
}

func NewKmutex() KMutex {
	l := new(sync.Mutex)
	return &kMutex{l: l, cond: sync.NewCond(l), mtxMap: make(map[interface{}]struct{})}
}

func (k kMutex) locked(key interface{}) (ok bool) {
	_, ok = k.mtxMap[key]
	return
}

func (k kMutex) Lock(key interface{}) {
	k.l.Lock()
	for k.locked(key) {
		k.cond.Wait()
	}
	k.mtxMap[key] = empty
	k.l.Unlock()
}

func (k kMutex) UnLock(key interface{}) {
	k.l.Lock()
	if _, ok := k.mtxMap[key]; ok {
		delete(k.mtxMap, key)
		k.cond.Broadcast()
	} else {
		fmt.Println(k.mtxMap)
		panic("unlock a unlocked lock")
	}
	k.l.Unlock()
}

//sync.map 实现 依然会有大量协程被唤醒，不过没有惊群影响大，不过应该获得锁的线程可能不能获得锁
type mapKMutex struct {
	s *sync.Map
}

func (mk mapKMutex) Lock(key interface{}) {
	m := &sync.Mutex{}                   //
	mLock, _ := mk.s.LoadOrStore(key, m) //
	mLock.(*sync.Mutex).Lock()           //
	if mLock.(*sync.Mutex) != m {
		mLock.(*sync.Mutex).Unlock() //
		mk.Lock(key)
		return
	}
	return
}

func (mk mapKMutex) UnLock(key interface{}) {
	if mLock, ok := mk.s.Load(key); !ok {
		panic("unlock an unlocked lock")
	} else {
		mk.s.Delete(key)
		mLock.(*sync.Mutex).Unlock()
	}
}

func NewMapKmutex() KMutex {
	return &mapKMutex{
		s: &sync.Map{},
	}
}
