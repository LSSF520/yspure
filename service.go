package aplrpc

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type //请求参数
	ReplyType reflect.Type //响应参数
	numCalls  uint64       //被调用的次数
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls) //统计方法被调用的次数
}

func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

func (m *methodType) newReplyv() reflect.Value {
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyv
}

// 结构体用于存储RPC服务的信息，包括结构体名称、类型、实例、方法列表
type service struct {
	name   string                 //映射的结构体本身
	typ    reflect.Type           //结构体的类型
	rcvr   reflect.Value          //结构体的实例
	method map[string]*methodType //结构体所有符合条件的方法
}

// 负责创建服务，并注册符合RPC规则的方法
func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)                  //获取结构体的映射值
	s.name = reflect.Indirect(s.rcvr).Type().Name() //获取结构体名称，用来映射服务名
	s.typ = reflect.TypeOf(rcvr)                    //获取反射类型，用于解析方法
	if !ast.IsExported(s.name) {                    //检查结构体是否是导出的
		log.Fatalf("rpc server：%s is not a valid service name", s.name)
	}
	s.registerMethods()
	return s
}

// 负责筛选和注册符合规则的方法，并存入service.method
func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", s.name, method.Name)
	}
}

// 通过反射调用具体的方法，并处理返回值
func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func
	returnValues := f.Call([]reflect.Value{s.rcvr, argv, replyv}) //反射调用的方法
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}

// 检查一个方法是否是导出的或者是内置的
func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == "" //如果是导出的则返回方法名称||Go 内置类型的 PkgPath 为空，因此可以用它判断是否是内置类型。
}
