package objmeta

import (
	"testing"

	"github.com/thanos-io/thanos/pkg/objmeta/objmetapb"
)

func Test_isBlockMeta(t *testing.T) {
	type args struct {
		objectName string
	}
	tests := []struct {
		name     string
		args     args
		isMeta   bool
		metaType objmetapb.Type
		blockID  string
	}{
		{
			name:     "meta.json",
			args:     args{objectName: "01H5HVEQ22RZ6PYNQHNJKCXZM9/meta.json"},
			isMeta:   true,
			metaType: objmetapb.Type_TYPE_META,
			blockID:  "01H5HVEQ22RZ6PYNQHNJKCXZM9",
		},
		{
			name:     "deletion-mark.json",
			args:     args{objectName: "01H5HTWHDTBH017ZSW8XGHV338/deletion-mark.json"},
			isMeta:   true,
			metaType: objmetapb.Type_TYPE_DELETE_MARK,
			blockID:  "01H5HTWHDTBH017ZSW8XGHV338",
		},
		{
			name:     "no-compact-mark.json",
			args:     args{objectName: "01H5HTWHDTBH017ZSW8XGHV338/no-compact-mark.json"},
			isMeta:   true,
			metaType: objmetapb.Type_TYPE_NO_COMPACT_MARK,
			blockID:  "01H5HTWHDTBH017ZSW8XGHV338",
		},
		{
			name:     "not meta.json",
			args:     args{objectName: "01H5HTWHDTBH017ZSW8XGHV338/chunks/"},
			isMeta:   false,
			metaType: objmetapb.Type_TYPE_UNKNOWN,
			blockID:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2 := isBlockMeta(tt.args.objectName)
			if got != tt.isMeta {
				t.Errorf("isBlockMeta() got = %v, want %v", got, tt.isMeta)
			}
			if got1 != tt.metaType {
				t.Errorf("isBlockMeta() got1 = %v, want %v", got1, tt.metaType)
			}
			if got2 != tt.blockID {
				t.Errorf("isBlockMeta() got2 = %v, want %v", got2, tt.blockID)
			}
		})
	}
}
