package mmf

import (
	"syscall"
	"unsafe"
)

type Mmap64 struct {
	addr  uintptr
	off   int64
	len   int64
	fd    int
	prot  int
	flags int
}

func mmap(addr, len, prot, flags, fd, offset uintptr) (uintptr, error) {
	xaddr, _, err := syscall.Syscall6(syscall.SYS_MMAP, addr, len, prot, flags, fd, offset)
	if err != 0 {
		return 0, err
	}
	return xaddr, nil
}

//# define MREMAP_MAYMOVE	1
const mREMAP_MAYMOVE = 1

func mremap(addr, len, lenNew uintptr) (uintptr, error) {
	xaddr, _, err := syscall.Syscall6(syscall.SYS_MREMAP, addr, len, lenNew, mREMAP_MAYMOVE, 0, 0)
	if err != 0 {
		return 0, err
	}
	return xaddr, nil
}

func munmap(addr, len uintptr) error {
	_, _, err := syscall.Syscall(syscall.SYS_MUNMAP, addr, len, 0)
	if err != 0 {
		return err
	}
	return nil
}

func Map64(fd int, off, len int64, prot, flags int) (*Mmap64, error) {
	m := &Mmap64{off: off, len: len, fd: fd, prot: prot, flags: flags}
	addr, err := mmap(0, uintptr(len), uintptr(prot), uintptr(flags),
		uintptr(fd), uintptr(off))
	if err != nil {
		return nil, err
	}
	m.addr = addr
	return m, nil
}

func (m *Mmap64) Len() int64 {
	return m.len
}

func (m *Mmap64) Offset() int64 {
	return m.off
}

// https://groups.google.com/forum/#!msg/golang-nuts/GC4i2lzoIPI/XLAR34cGLTQJ
func (m *Mmap64) Slice(off int64, len int) []byte {
	var sl = struct {
		addr uintptr
		len  int
		cap  int
	}{m.addr + uintptr(off), len, len}
	b := *(*[]byte)(unsafe.Pointer(&sl))
	return b
}

func (m *Mmap64) Unmap() error {
	return munmap(m.addr, uintptr(m.len))
}
