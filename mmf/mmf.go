package mmf

import (
	"errors"
	"os"
	"syscall"
	"unsafe"
)

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

var (
	ErrFileEnd       = errors.New("File end reached")
	ErrAlreadyMapped = errors.New("Mmf already mapped")
	ErrNonTmpRename  = errors.New("File path is not tmp")
)

const MMF_FILE_BEGIN = 256 * 256

func FileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return false
}

func Unlink(path string) error {
	return os.Remove(path)
}

// memory mapping and memory mapped file, only unix compatible

// memory mapped file

type Mmf struct {
	File     *os.File // File - os.File, here a Linux File
	FileSize int64    // FileSize - is the length in bytes for regular files
	MapSize  int64    // MapSize - is the length in bytes for the mmapped region

	PosWrite uintptr // is the position of the writer, must be determined individually for Writer
	PosFlush uintptr // is the position of the writer when last flush occured
	Mmap     *Mmap64

	path string
}

func (mmf *Mmf) NullFile() {
	C.memset(unsafe.Pointer(mmf.Mmap.addr), 0, C.size_t(mmf.FileSize))

}

func (mmf *Mmf) AddrBegin() uintptr {
	return mmf.Mmap.addr
}

func (mmf *Mmf) IsMapped() bool {
	return mmf.Mmap != nil && mmf.Mmap.addr != 0
}

func (mmf *Mmf) GetPath() string {
	return mmf.path
}

func (mmf *Mmf) Close() {
	mmf.CloseForReopen()
	mmf.File = nil
	mmf.path = ""
}

func (mmf *Mmf) CloseForReopen() {
	munmap(mmf.Mmap.addr, uintptr(mmf.MapSize))
	mmf.File.Close()
}

func (mmf *Mmf) Madvise(adv int) {
	syscall.Syscall(syscall.SYS_MADVISE, mmf.Mmap.addr, uintptr(mmf.MapSize), uintptr(adv))
}

func (mmf *Mmf) OpenAndMapReader(path string, mapSize int64) error {
	if mmf.IsMapped() {
		return ErrAlreadyMapped
	}

	file, err := os.Open(path)

	if err != nil {
		return err
	}
	stats, err := file.Stat()
	if err != nil {
		return err
	}

	fileSize := stats.Size()
	var mmap *Mmap64

	if mapSize <= 0 {
		mmap, err = Map64(int(file.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			return err
		}
		mmf.MapSize = fileSize
	} else {
		mmap, err = Map64(int(file.Fd()), 0, mapSize, syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			return err
		}
		mmf.MapSize = mapSize
	}

	mmf.File = file
	mmf.Mmap = mmap
	mmf.FileSize = fileSize
	mmf.PosWrite = MMF_FILE_BEGIN

	mmf.path = path
	return nil
}

/*
func (mmf *Mmf) SyncSure() error {
	err := mmf.File.Sync()
	if err != nil {
		return err
	}
	return mmf.File.Sync()
}
*/

func (mmf *Mmf) SyncSure() error {
	return mmf.File.Sync()
}

func (mmf *Mmf) UnlinkClose() {
	Unlink(mmf.path)
	mmf.Close()
	mmf.PosFlush = uintptr(0)
	mmf.PosWrite = uintptr(0)
}

func (mmf *Mmf) RenameFromTmp() error {
	npath := mmf.path[len(mmf.path)-4:]
	if npath != "_tmp" {
		return ErrNonTmpRename
	}
	npath = mmf.path[:len(mmf.path)-4]

	err := os.Rename(mmf.path, npath)
	if err != nil {
		return err
	}
	mmf.path = npath
	return err
}

func (mmf *Mmf) Rename(pathNew string) error {
	err := os.Rename(mmf.path, pathNew)
	if err != nil {
		return err
	}
	mmf.path = pathNew

	return nil
}

func (mmf *Mmf) OpenAndMapWriter(path string, sz int64, create bool) (bool, error) {
	if mmf.IsMapped() {
		return false, ErrAlreadyMapped
	}

	created := false

	perm := os.O_RDWR
	if create {
		perm |= os.O_CREATE
	}

	file, err := os.OpenFile(path, perm, 0777)
	if err != nil {
		return false, err
	}

	if sz > MMF_FILE_BEGIN {
		r := sz % MMF_FILE_BEGIN
		if r != 0 {
			sz = sz - r + MMF_FILE_BEGIN
		}
	} else {
		sz = MMF_FILE_BEGIN
	}

	stats, err := file.Stat()
	if err != nil {
		return false, err
	}

	fileSize := stats.Size()
	if fileSize < sz {
		err = file.Truncate(sz)
		if err != nil {
			return created, err
		}
		if fileSize == 0 {
			created = true
		}
		fileSize = sz
	}
	//todo truncate and/or create
	var mmap *Mmap64

	mmap, err = Map64(int(file.Fd()), 0, fileSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return created, err
	}
	mmf.MapSize = fileSize

	mmf.File = file
	mmf.Mmap = mmap
	mmf.FileSize = fileSize
	mmf.PosWrite = MMF_FILE_BEGIN

	mmf.path = path

	return created, nil
}

func (mmf *Mmf) EnsureSizeWriter(sz int64) (bool, error) {
	if mmf.FileSize > sz {
		return false, nil
	}
	//sz = (MMF_FILE_BEGIN - sz%MMF_FILE_BEGIN) + sz
	r := sz % MMF_FILE_BEGIN
	if r != 0 {
		sz = sz - r + MMF_FILE_BEGIN
	}

	err := syscall.Fallocate(int(mmf.File.Fd()), 0, mmf.FileSize, sz)
	if err != nil {
		return false, err
	}

	addrNew, err := mremap(mmf.Mmap.addr, uintptr(mmf.FileSize), uintptr(sz))
	if err != nil {
		return false, err
	}

	mmf.Mmap.addr = addrNew
	mmf.MapSize = sz
	mmf.FileSize = sz

	return false, nil
}
