package srchx

import (
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/blevesearch/bleve"
)

// Store our main store wrapper
type Store struct {
	datapath    string
	indexes     map[string]*Index
	indexesLock sync.RWMutex
}

// NewStore initialize a new store using the specified path, supports only leveldb
func NewStore(path string) (*Store, error) {
	s := new(Store)
	s.datapath = filepath.Join(path, "leveldb")
	s.indexes = map[string]*Index{}
	s.indexesLock = sync.RWMutex{}

	os.MkdirAll(s.datapath, 0744)

	return s, nil
}

// GetIndex load/init an index and return it
func (s *Store) GetIndex(name string) (*Index, error) {
	var err error

	name = strings.ToLower(name)
	ndx, ok := s.indexes[name]

	if !ok {
		ndx, err = s.InitIndex(name)
	}

	if err != nil {
		return nil, err
	}

	return ndx, nil
}

// InitIndex create an index and register it in our main registry
func (s *Store) InitIndex(name string) (ndx *Index, err error) {
	name = strings.ToLower(name)
	indexPath := path.Join(s.datapath, name)

	s.indexesLock.Lock()
	defer s.indexesLock.Unlock()

	if err = os.MkdirAll(indexPath, 0744); err != nil && err != os.ErrExist {
		return nil, err
	}

	indexMapping := bleve.NewIndexMapping()
	ndx, err = initLevelIndex(indexPath, indexMapping)

	if err != nil {
		return nil, err
	}

	s.indexes[name] = ndx

	return ndx, err
}
