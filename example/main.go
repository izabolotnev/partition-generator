package main

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	partitionGenerator "github.com/izabolotnev/partition-generator"
)

func main() {
	time.Local = time.UTC
	from, _ := time.Parse("2006 Jan 02 15:04:05", "2022 Jan 01 01:00:31.918273645")
	to, _ := time.Parse("2006 Jan 02 15:04:05", "2022 Mar 20 01:00:31.918273645")

	generator := partitionGenerator.PartitionGenerator{
		LoadPartitionNames:  loadPartitionNames,
		TimeToMilestone:     timeToMilestone,
		TimeToNextMilestone: timeToNextMilestone,
		MilestoneToName:     milestoneToPartitionName,
		NameToMilestone:     partitionNameToMilestone,
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

func timeToMilestone(now time.Time) time.Time {
	now = now.Truncate(24 * time.Hour)
	switch now.Weekday() {
	case time.Monday:
		return now
	case time.Sunday:
		return now.AddDate(0, 0, -6)
	default:
		return now.AddDate(0, 0, -(int(now.Weekday()) - 1))
	}
}

func timeToNextMilestone(now time.Time) time.Time {
	return now.AddDate(0, 0, 7)
}

func milestoneToPartitionName(v time.Time) string {
	return fmt.Sprintf("prefix_y%dm%02dd%02d", v.Year(), v.Month(), v.Day())
}

func partitionNameToMilestone(v string) *time.Time {
	r := regexp.MustCompile(`prefix_y(\d{4})m(\d{2})d(\d{2})`)
	submatch := r.FindStringSubmatch(v)

	if len(submatch) == 0 {
		return nil
	}

	submatch = submatch[1:]
	year, _ := strconv.Atoi(submatch[0])
	month, _ := strconv.Atoi(submatch[1])
	day, _ := strconv.Atoi(submatch[2])
	milestone := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	return &milestone
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
