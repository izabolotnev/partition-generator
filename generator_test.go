package partition_generator

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestPartitionGenerator_Generate(t *testing.T) {
	exists := []string{
		"prefix_y2021m12d30",
		"prefix_y2021m12d31",
		"prefix_y2022m01d01",
		"prefix_y2022m01d03",
		"prefix_y3022m12d01",
	}
	expect := []string{
		"prefix_y2021m12d30 delete",
		"prefix_y2021m12d31 delete",
		"prefix_y2022m01d01 skip",
		"prefix_y2022m01d02 add",
		"prefix_y2022m01d03 skip",
		"prefix_y3022m12d01 delete",
	}
	var result []string

	p := PartitionGenerator{
		LoadPartitionNames: func(_ context.Context) []string {
			return exists
		},
		TimeToMilestone: func(t time.Time) time.Time {
			return t.Truncate(24 * time.Hour)
		},
		TimeToNextMilestone: func(t time.Time) time.Time {
			return t.AddDate(0, 0, 1)
		},
		MilestoneToName: func(t time.Time) string {
			return t.Format("prefix_y2006m01d02")
		},
		NameToMilestone: func(name string) *time.Time {
			t, _ := time.Parse("prefix_y2006m01d02", name)
			return &t
		},
		CreatePartition: func(_ context.Context, name string) {
			result = append(result, name+" add")
		},
		DeletePartition: func(_ context.Context, name string) {
			result = append(result, name+" delete")
		},
		SkipPartition: func(_ context.Context, name string) {
			result = append(result, name+" skip")
		},
	}
	from, _ := time.Parse("2006-01-02T15:04:05", "2022-01-01T16:17:18")
	to, _ := time.Parse("2006-01-02T15:04:05", "2022-01-03T16:17:18")

	p.Generate(context.Background(), from, to)

	sort.Strings(expect)
	sort.Strings(result)

	if !reflect.DeepEqual(result, expect) {
		t.Logf("\nresult: %v\nexpect: %v", result, expect)
		t.Fail()
	}
}
