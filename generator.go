package partition_generator

import (
	"context"
	"time"
)

type decision int

const (
	decisionAdd decision = iota
	decisionDelete
	decisionSkip
)

type milestoneDecision struct {
	milestone time.Time
	decision
}

type PartitionGenerator struct {
	LoadPartitionNames func(ctx context.Context) []string

	TimeToMilestone     func(time.Time) time.Time
	TimeToNextMilestone func(time.Time) time.Time

	MilestoneToName func(time.Time) string
	NameToMilestone func(string) *time.Time

	CreatePartition func(context.Context, string)
	DeletePartition func(context.Context, string)
	SkipPartition   func(context.Context, string)
}

func (p *PartitionGenerator) Generate(ctx context.Context, from, to time.Time) {
	tables := p.LoadPartitionNames(ctx)
	existMilestones := p.partitionNamesToMilestones(ctx, tables)
	targetMilestones := p.generateTargetMilestones(from, to)

	decisions := p.intersectMilestones(existMilestones, targetMilestones)

	for i := range decisions {
		partitionName := p.MilestoneToName(decisions[i].milestone)
		switch decisions[i].decision {
		case decisionAdd:
			p.CreatePartition(ctx, partitionName)
		case decisionDelete:
			p.DeletePartition(ctx, partitionName)
		case decisionSkip:
			p.SkipPartition(ctx, partitionName)
		}
	}
}

func (p *PartitionGenerator) partitionNamesToMilestones(_ context.Context, tables []string) []time.Time {
	result := make([]time.Time, 0, len(tables))
	for i := range tables {
		milestone := p.NameToMilestone(tables[i])
		if milestone != nil {
			result = append(result, *milestone)
		}
	}

	return result
}

func (p *PartitionGenerator) generateTargetMilestones(from, to time.Time) []time.Time {
	currentPartition := p.TimeToMilestone(from)

	result := []time.Time{currentPartition}
	for {
		currentPartition = p.TimeToNextMilestone(currentPartition)
		if currentPartition.After(to) {
			break
		}
		result = append(result, currentPartition)
	}

	return result
}

func (p *PartitionGenerator) intersectMilestones(actualMilestones, targetMilestones []time.Time) []milestoneDecision {
	var t, a int
	var result []milestoneDecision
	for {
		if a >= len(actualMilestones) {
			for _, v := range targetMilestones[t:] {
				result = append(result, milestoneDecision{
					milestone: v,
					decision:  decisionAdd,
				})
			}
			break
		}
		if t >= len(targetMilestones) {
			for _, v := range actualMilestones[a:] {
				result = append(result, milestoneDecision{
					milestone: v,
					decision:  decisionDelete,
				})
			}
			break
		}

		if actualMilestones[a].Before(targetMilestones[t]) {
			result = append(result, milestoneDecision{
				milestone: actualMilestones[a],
				decision:  decisionDelete,
			})
			a++
			continue
		}
		if targetMilestones[t].Before(actualMilestones[a]) {
			result = append(result, milestoneDecision{
				milestone: targetMilestones[t],
				decision:  decisionAdd,
			})
			t++
			continue
		}
		if actualMilestones[a].Equal(targetMilestones[t]) {
			result = append(result, milestoneDecision{
				milestone: actualMilestones[a],
				decision:  decisionSkip,
			})
			t++
			a++
			continue
		}
	}
	return result
}
