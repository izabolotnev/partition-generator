package week

import "time"

func TimeToMilestone(now time.Time) time.Time {
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

func TimeToNextMilestone(now time.Time) time.Time {
	return now.AddDate(0, 0, 7)
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
