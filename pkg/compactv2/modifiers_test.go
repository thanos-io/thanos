package compactv2

import (
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"reflect"
	"testing"
)

func TestIntersection(t *testing.T) {
	type args struct {
		i       tombstones.Interval
		dranges tombstones.Intervals
	}
	tests := []struct {
		name string
		args args
		want tombstones.Intervals
	}{
		{
			name: "test",
			args: args{i: tombstones.Interval{Maxt: 10, Mint: 0},
				dranges: tombstones.Intervals{{Maxt: 12, Mint: 5}}},
			want: tombstones.Intervals{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Intersection(tt.args.i, tt.args.dranges); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Intersection() = %v, want %v", got, tt.want)
			}
		})
	}
}
