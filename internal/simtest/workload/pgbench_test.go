package workload

import "testing"

func TestParseTPS(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want float64
	}{
		{
			name: "standard progress line",
			in:   "progress: 5.0 s, 1234.5 tps, lat 8.123 ms stddev 2.345",
			want: 1234.5,
		},
		{
			name: "integer tps",
			in:   "progress: 10.0 s, 500 tps, lat 4.0 ms stddev 1.0",
			want: 500,
		},
		{
			name: "zero tps",
			in:   "progress: 5.0 s, 0 tps, lat 0 ms stddev 0",
			want: 0,
		},
		{
			name: "high tps",
			in:   "progress: 5.0 s, 98765.43 tps, lat 0.5 ms",
			want: 98765.43,
		},
		{
			name: "no tps token",
			in:   "some other line entirely",
			want: 0,
		},
		{
			name: "tps without number",
			in:   "progress: tps nonsense",
			want: 0,
		},
		{
			name: "empty",
			in:   "",
			want: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseTPS(tc.in)
			if got != tc.want {
				t.Errorf("parseTPS(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
