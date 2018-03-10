/*一个专门用于保存记录的文件，特点是保存时，可以用map类型，自动收集字段清单，然后每行只存放一个数组
文件结构zip：
data.dat
 	[]interface{}  	--记录1
	[]interface{}  	--记录2
	...
info.dat
	[]string{}		--字段清单
	uint64			--记录总数
*/
package recfile

import (
	"archive/zip"
	"encoding/gob"
	"io"
)

func init() {
	gob.Register(map[string]interface{}{})
	gob.Register(new(Info))
}

type Info struct {
	Columns     []string
	RecordCount uint64
	Tag         map[string]interface{}
}
type Encoder struct {
	count      uint64
	columnsMap map[string]bool
	tag        map[string]interface{}
	columns    []string
	values     []interface{}
	gobend     *gob.Encoder
	dataWriter io.Writer
	zipw       *zip.Writer
}

func NewEncoder(s io.WriteCloser, tag map[string]interface{}) (*Encoder, error) {
	zipw := zip.NewWriter(s)
	w, err := zipw.Create("data.dat")
	if err != nil {
		return nil, err
	}
	return &Encoder{
		tag:        tag,
		columns:    []string{},
		columnsMap: map[string]bool{},
		values:     []interface{}{},
		zipw:       zipw,
		dataWriter: w,
	}, nil
}
func (e *Encoder) Write(rec map[string]interface{}) error {
	if e.gobend == nil {
		e.gobend = gob.NewEncoder(e.dataWriter)
	}
	//将map转换成数组,首先找出未登记的字段名称进行登记
	for k, _ := range rec {
		if _, ok := e.columnsMap[k]; !ok {
			e.columns = append(e.columns, k)
			e.columnsMap[k] = true
		}
	}
	//然后依次将属性转换成数组
	e.values = e.values[:0]
	preNotNilIdx := -1
	for i, col := range e.columns {
		if rec[col] != nil {
			preNotNilIdx = i
		}
		e.values = append(e.values, rec[col])
	}
	//移除末尾的nil
	if preNotNilIdx < len(e.columns)-1 {
		e.values = e.values[0 : preNotNilIdx+1]
	}
	if err := e.gobend.Encode(e.values); err != nil {
		return err
	}
	e.count++
	return nil
}
func (e *Encoder) Close() error {

	w, err := e.zipw.Create("info.dat")

	if err != nil {
		return err
	}
	info := &Info{
		Columns:     e.columns,
		RecordCount: e.count,
		Tag:         e.tag,
	}
	if err = gob.NewEncoder(w).Encode(info); err != nil {
		return err
	}
	return e.zipw.Close()
}
