package month

import "time"

func TimeToMilestone(now time.Time) time.Time {
	return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.Local)
}

func TimeToNextMilestone(now time.Time) time.Time {
	return now.AddDate(0, 1, 0)
}

func MilestoneToPartitionName(layout string, v time.Time) string {
	return v.Format(layout)
}

func PartitionNameToMilestone(layout string, v string) *time.Time {
	milestone, err := time.Parse(layout, v)
	if err != nil {
		return nil
	}
	return &milestone
}
