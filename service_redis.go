package basalt

import (
	"strconv"
	"strings"

	"github.com/tidwall/redcon"
)

// RedisService is a redis service only supports bitmap commands.
type RedisService struct {
	s *Server
}

func (rs *RedisService) redisAccept(conn redcon.Conn) bool {
	return true
}
func (rs *RedisService) redisClose(conn redcon.Conn, err error) {
}

// redisHandler handles redis commands.
func (rs *RedisService) redisHandler(conn redcon.Conn, cmd redcon.Command) {
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
	case "ping":
		conn.WriteString("PONG")
	case "quit":
		conn.WriteString("OK")
		conn.Close()
	case "bmadd": // bitmap add
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		v, err := byte2uint32(cmd.Args[2])
		if err != nil {
			conn.WriteError("ERR wrong value for '" + string(cmd.Args[0]) + "' command because of " + err.Error())
			return
		}

		rs.s.bitmaps.Add(string(cmd.Args[1]), v)
		conn.WriteInt(1)

	case "bmaddmany": // bitmap addmany
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		values, err := bytes2uint32(cmd.Args[2:])
		if err != nil {
			conn.WriteError("ERR wrong value for '" + string(cmd.Args[0]) + "' command because of " + err.Error())
			return
		}

		rs.s.bitmaps.AddMany(string(cmd.Args[1]), values)
		conn.WriteInt(len(values))

	case "bmdel": // bitmap remove
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		v, err := byte2uint32(cmd.Args[2])
		if err != nil {
			conn.WriteError("ERR wrong value for '" + string(cmd.Args[0]) + "' command because of " + err.Error())
			return
		}

		rs.s.bitmaps.Remove(string(cmd.Args[1]), v)
		conn.WriteInt(1)

	case "bmdrop": // bitmap remove_bitmap
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		rs.s.bitmaps.RemoveBitmap(string(cmd.Args[1]))
		conn.WriteString("OK")

	case "bmclear": // bitmap clear_bitmap
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		rs.s.bitmaps.ClearBitmap(string(cmd.Args[1]))
		conn.WriteString("OK")
	case "bmcard": // bitmap clear_bitmap
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		count := rs.s.bitmaps.Card(string(cmd.Args[1]))
		conn.WriteInt64(int64(count))

	case "bmexists": // bitmap exists
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		v, err := byte2uint32(cmd.Args[2])
		if err != nil {
			conn.WriteError("ERR wrong value for '" + string(cmd.Args[0]) + "' command because of " + err.Error())
			return
		}

		existed := rs.s.bitmaps.Exists(string(cmd.Args[1]), v)
		if existed {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}

	case "bminter": // bitmap intersect
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		names := bytes2string(cmd.Args[1:])
		rt := rs.s.bitmaps.Inter(names...)

		conn.WriteArray(len(rt))
		for _, v := range rt {
			conn.WriteInt64(int64(v))
		}

	case "bminterstore": // bitmap intersect store
		if len(cmd.Args) < 4 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		names := bytes2string(cmd.Args[1:])
		count := rs.s.bitmaps.InterStore(names[0], names[1:]...)
		conn.WriteInt64(int64(count))

	case "bmunion": // bitmap union
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		names := bytes2string(cmd.Args[1:])
		rt := rs.s.bitmaps.Union(names...)

		conn.WriteArray(len(rt))
		for _, v := range rt {
			conn.WriteInt64(int64(v))
		}
	case "bmunionstore": // bitmap union store
		if len(cmd.Args) < 4 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		names := bytes2string(cmd.Args[1:])
		count := rs.s.bitmaps.UnionStore(names[0], names[1:]...)
		conn.WriteInt64(int64(count))

	case "bmxor": // bitmap xor
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		rt := rs.s.bitmaps.Xor(string(cmd.Args[1]), string(cmd.Args[2]))

		conn.WriteArray(len(rt))
		for _, v := range rt {
			conn.WriteInt64(int64(v))
		}
	case "bmxorstore": // bitmap xor store
		if len(cmd.Args) != 4 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		count := rs.s.bitmaps.XorStore(string(cmd.Args[1]), string(cmd.Args[2]), string(cmd.Args[3]))
		conn.WriteInt64(int64(count))

	case "bmdiff": // bitmap diff
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		rt := rs.s.bitmaps.Diff(string(cmd.Args[1]), string(cmd.Args[2]))

		conn.WriteArray(len(rt))
		for _, v := range rt {
			conn.WriteInt64(int64(v))
		}
	case "bmdiffstore": // bitmap diff store
		if len(cmd.Args) != 4 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		count := rs.s.bitmaps.DiffStore(string(cmd.Args[1]), string(cmd.Args[2]), string(cmd.Args[3]))
		conn.WriteInt64(int64(count))
	case "bmstats": // bitmap diff store
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		stats := rs.s.bitmaps.Stats(string(cmd.Args[1]))

		var sb strings.Builder
		appendMetric(&sb, "cardinality", stats.Cardinality)
		appendMetric(&sb, "Containers", stats.Containers)

		appendMetric(&sb, "ArrayContainers", stats.ArrayContainers)
		appendMetric(&sb, "ArrayContainerBytes", stats.ArrayContainerBytes)
		appendMetric(&sb, "ArrayContainerValues", stats.ArrayContainerValues)

		appendMetric(&sb, "BitmapContainers", stats.BitmapContainers)
		appendMetric(&sb, "BitmapContainerBytes", stats.BitmapContainerBytes)
		appendMetric(&sb, "BitmapContainerValues", stats.BitmapContainerValues)

		appendMetric(&sb, "RunContainers", stats.RunContainers)
		appendMetric(&sb, "RunContainerBytes", stats.RunContainerBytes)
		appendMetric(&sb, "RunContainerValues", stats.RunContainerValues)

		conn.WriteBulkString(sb.String())
	case "bmsave": // bitmap persist
		if len(cmd.Args) != 1 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		err := rs.s.Save()
		if err != nil {
			conn.WriteError("ERR save because of " + err.Error())
			return
		}

		conn.WriteInt(1)
	}
}

func appendMetric(sb *strings.Builder, name string, v uint64) {
	sb.WriteString(name)
	sb.WriteString(":")
	sb.WriteString(strconv.FormatUint(v, 10))
	sb.WriteString("\r\n")
}

func byte2uint32(b []byte) (uint32, error) {
	i, err := strconv.ParseUint(string(b), 10, 32)
	return uint32(i), err
}

func bytes2uint32(b [][]byte) ([]uint32, error) {
	var rt []uint32
	for _, bt := range b {
		i, err := strconv.ParseUint(string(bt), 10, 32)
		if err != nil {
			return nil, err
		}
		rt = append(rt, uint32(i))
	}
	return rt, nil
}

func bytes2string(b [][]byte) []string {
	var rt []string
	for _, bt := range b {
		rt = append(rt, string(bt))
	}
	return rt
}
