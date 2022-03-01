package main

import (
	"context"
	"fmt"
	"time"

	partitionGenerator "github.com/izabolotnev/partition-generator"
	"github.com/izabolotnev/partition-generator/week"
)

func main() {
	time.Local = time.UTC
	from, _ := time.Parse("2006 Jan 02 15:04:05", "2022 Jan 01 01:00:31.918273645")
	to, _ := time.Parse("2006 Jan 02 15:04:05", "2022 Mar 20 01:00:31.918273645")

	generator := partitionGenerator.PartitionGenerator{
		LoadPartitionNames:  loadPartitionNames,
		TimeToMilestone:     week.TimeToMilestone,
		TimeToNextMilestone: week.TimeToNextMilestone,
		MilestoneToName:     milestoneToPartitionName("prefix_y2006m01d02"),
		NameToMilestone:     partitionNameToMilestone("prefix_y2006m01d02"),
		CreatePartition:     createPartition,
		DeletePartition:     deletePartition,
		SkipPartition:       skipPartition,
	}

	generator.Generate(context.Background(), from, to)
}

func loadPartitionNames(_ context.Context) []string {
	return []string{
		"prefix_y2021m12d06",
		"prefix_y2021m12d13",
		"prefix_y2021m12d20",
		"prefix_y2021m12d27",
		"prefix_y2022m01d03",
		"prefix_y2022m01d10",
		"prefix_y2022m01d17",
		"prefix_y2022m01d24",
		//"prefix_y2022m01d31",
		"prefix_y2022m02d07",
		"prefix_y2022m02d14",
		"prefix_y2022m02d21",
		"prefix_y2022m02d28",
		"prefix_y3022m02d28",
	}
}

func milestoneToPartitionName(layout string) func(v time.Time) string {
	return func(v time.Time) string {
		return week.MilestoneToPartitionName(layout, v)
	}
}

func partitionNameToMilestone(layout string) func(v string) *time.Time {
	return func(v string) *time.Time {
		return week.PartitionNameToMilestone(layout, v)
	}
}

func createPartition(_ context.Context, name string) {
	fmt.Println(name + " add")
}

func deletePartition(_ context.Context, name string) {
	fmt.Println(name + " delete")
}

func skipPartition(_ context.Context, name string) {
	fmt.Println(name + " skip")
}
