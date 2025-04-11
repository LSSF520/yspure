package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type GobCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	dec  *gob.Decoder
	enc  *gob.Encoder
}

var _Codec = (*GobCodec)(nil)

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf),
	}
}

// 实现Codec方法：
func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h) //将header结构解析出来
}

func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body) //将body的内容解析出来
}

func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() { //在write方法返回前执行
		_ = c.buf.Flush() //确保缓冲区数据写入底层io
		if err != nil {   //访问函数声明里的err
			_ = c.Close() //如果发生错误，关闭连接
		}
	}()

	if err := c.enc.Encode(h); err != nil {
		log.Println("rpc codec:gob error encoding header:", err)
		return err
	}
	if err := c.enc.Encode(body); err != nil {
		log.Println("rpc codec:gob error encoding body:", err)
		return err
	}
	return
}

func (c *GobCodec) Close() error {
	return c.conn.Close()
}
