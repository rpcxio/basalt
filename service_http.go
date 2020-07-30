package basalt

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"
)

// HTTPService is a http service.
type HTTPService struct {
	router             *httprouter.Router
	s                  *Server
	confChangeCallback ConfChange
}

// Serve serves http service.
func (s *HTTPService) Serve(ln net.Listener) error {
	s.config()

	return http.Serve(ln, s.router)
}

func (s *HTTPService) config() {
	router := httprouter.New()
	s.router = router

	router.POST("/add/:name/:value", s.add)
	router.POST("/addmany/:name/:values", s.addMany)
	router.POST("/remove/:name/:value", s.remove)
	router.POST("/drop/:name", s.drop)
	router.POST("/clear/:name", s.clear)
	router.GET("/exists/:name/:value", s.exists)
	router.GET("/card/:name", s.card)

	router.GET("/inter/:names", s.inter)
	router.GET("/interstore/:dst/:names", s.interStore)

	router.GET("/union/:names", s.union)
	router.GET("/unionstore/:dst/:names", s.unionStore)

	router.GET("/xor/:name1/:name2", s.xor)
	router.GET("/xorstore/:dst/:name1/:name2", s.xorStore)

	router.GET("/diff/:name1/:name2", s.diff)
	router.GET("/diffstore/:dst/:name1/:name2", s.diffStore)

	router.GET("/stats/:name", s.stats)
	router.POST("/save", s.save)

	router.POST("/peers/:nodeID", s.addNode)
	router.DELETE("/peers/:nodeID", s.removeNode)
}

func (s *HTTPService) add(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	value := ps.ByName("value")
	err := s.s.add(name, value, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func (s *HTTPService) addMany(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	values := ps.ByName("values")
	err := s.s.addMany(name, values, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func (s *HTTPService) remove(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	value := ps.ByName("value")
	err := s.s.remove(name, value, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func (s *HTTPService) drop(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	s.s.drop(name, true)
}

func (s *HTTPService) clear(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	s.s.clear(name, true)
}

func (s *HTTPService) card(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	count := s.s.bitmaps.Card(name)
	w.Write([]byte(strconv.FormatUint(count, 10)))
}

func (s *HTTPService) exists(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	value := ps.ByName("value")
	v, err := str2uint32(value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	existed := s.s.bitmaps.Exists(name, v)
	if !existed {
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (s *HTTPService) inter(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	names := strings.Split(ps.ByName("name"), ",")
	rt := s.s.bitmaps.Inter(names...)

	w.Write([]byte(ints2str(rt)))
}

func (s *HTTPService) interStore(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dst := ps.ByName("dst")
	names := strings.Split(ps.ByName("name"), ",")
	count := s.s.bitmaps.InterStore(dst, names...)

	w.Write([]byte(strconv.FormatUint(count, 10)))
}

func (s *HTTPService) union(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	names := strings.Split(ps.ByName("name"), ",")
	rt := s.s.bitmaps.Union(names...)

	w.Write([]byte(ints2str(rt)))
}

func (s *HTTPService) unionStore(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dst := ps.ByName("dst")
	names := strings.Split(ps.ByName("name"), ",")
	count := s.s.bitmaps.UnionStore(dst, names...)

	w.Write([]byte(strconv.FormatUint(count, 10)))
}

func (s *HTTPService) xor(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name1 := ps.ByName("name1")
	name2 := ps.ByName("name2")
	rt := s.s.bitmaps.Xor(name1, name2)

	w.Write([]byte(ints2str(rt)))
}

func (s *HTTPService) xorStore(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dst := ps.ByName("dst")
	name1 := ps.ByName("name1")
	name2 := ps.ByName("name2")
	count := s.s.bitmaps.XorStore(dst, name1, name2)

	w.Write([]byte(strconv.FormatUint(count, 10)))
}

func (s *HTTPService) diff(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name1 := ps.ByName("name1")
	name2 := ps.ByName("name2")
	rt := s.s.bitmaps.Diff(name1, name2)

	w.Write([]byte(ints2str(rt)))
}

func (s *HTTPService) diffStore(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	dst := ps.ByName("dst")
	name1 := ps.ByName("name1")
	name2 := ps.ByName("name2")
	count := s.s.bitmaps.DiffStore(dst, name1, name2)

	w.Write([]byte(strconv.FormatUint(count, 10)))
}

func (s *HTTPService) stats(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")
	stats := s.s.bitmaps.Stats(name)
	w.Header().Set("Content-Type", "application/json")
	data, err := json.Marshal(stats)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Write(data)
}

func (s *HTTPService) save(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	err := s.s.Save()
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
	}
}

func (s *HTTPService) addNode(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	nodeID := ps.ByName("nodeID")
	url, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed on POST", http.StatusBadRequest)
		return
	}
	id, err := strconv.ParseUint(nodeID, 0, 64)
	if err != nil {
		http.Error(w, "Failed on convert ID", http.StatusBadRequest)
		return
	}

	if s.confChangeCallback != nil {
		err = s.confChangeCallback.AddNode(id, url)
		if err != nil {
			http.Error(w, "failed to add node: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func (s *HTTPService) removeNode(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	nodeID := ps.ByName("nodeID")
	id, err := strconv.ParseUint(nodeID, 0, 64)
	if err != nil {
		http.Error(w, "Failed on convert ID", http.StatusBadRequest)
		return
	}

	if s.confChangeCallback != nil {
		err = s.confChangeCallback.RemoveNode(id)
		if err != nil {
			http.Error(w, "failed to remove node: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func ints2str(vs []uint32) string {
	// return strings.Trim(strings.Join(strings.Fields(fmt.Sprint(vs)), ","), "[]")
	return strings.Join(strings.Fields(fmt.Sprint(vs)), ",")
}

func str2uint32(s string) (uint32, error) {
	i, err := strconv.ParseUint(s, 10, 32)
	return uint32(i), err
}

func str2uint32s(s string) ([]uint32, error) {
	var rt []uint32
	b := strings.Split(s, ",")
	for _, bt := range b {
		i, err := strconv.ParseUint(bt, 10, 32)
		if err != nil {
			return nil, err
		}
		rt = append(rt, uint32(i))
	}
	return rt, nil
}
