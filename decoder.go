package recfile

import (
	"archive/zip"
	"encoding/gob"
	"fmt"
	"io"
	"log"

	"github.com/linlexing/mapfun"
)

type Decoder struct {
	columns     []string
	RecordCount uint64
	Tag         map[string]interface{}
	buf         []interface{}
	dec         *gob.Decoder
	zipr        *zip.ReadCloser
	dataReader  io.ReadCloser
}
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

func NewDecoder(fileName string) (*Decoder, error) {
	r, err := zip.OpenReader(fileName)
	if err != nil {
		return nil, err
	}

	var infoFile *zip.File
	var dataFile *zip.File
	info := new(Info)
	for _, file := range r.File {
		switch file.Name {
		case "info.dat":
			infoFile = file
		case "data.dat":
			dataFile = file
		}
	}
	//先读出字段清单和记录总数
	if infoFile == nil {
		return nil, fmt.Errorf("can't find the info.dat file.")
	}
	if dataFile == nil {
		return nil, fmt.Errorf("can't find the data.dat file.")
	}
	var infoReader io.ReadCloser
	var dataReader io.ReadCloser

	infoReader, err = infoFile.Open()
	if err != nil {
		return nil, err
	}
	defer infoReader.Close()
	if err = gob.NewDecoder(infoReader).Decode(info); err != nil {
		log.Println("decode info error", err, infoFile, infoFile.UncompressedSize64)
		return nil, err
	}
	//打开数据文件
	dataReader, err = dataFile.Open()
	if err != nil {
		log.Println("open data file error", err)
		return nil, err
	}

	return &Decoder{
		Tag:         info.Tag,
		buf:         []interface{}{},
		columns:     info.Columns,
		RecordCount: info.RecordCount,
		dec:         gob.NewDecoder(dataReader),
		zipr:        r,
		dataReader:  dataReader,
	}, nil
}
func (d *Decoder) Close() error {
	err := d.dataReader.Close()
	if err != nil {
		return err
	}
	return d.zipr.Close()
}
func (d *Decoder) Read() (map[string]interface{}, error) {
	d.buf = d.buf[:0]
	if err := d.dec.Decode(&d.buf); err != nil {
		return nil, err
	}
	return mapfun.Object(d.columns[:len(d.buf)], d.buf), nil
}
