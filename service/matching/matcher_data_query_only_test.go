package matching

func (s *MatcherDataSuite) TestPollerPQAllQueryOnly() {
	queryOnlyPoller := &waitingPoller{queryOnly: true}
	regularPoller := &waitingPoller{}

	// A pre-populated heap is initialized on its first allQueryOnly check.
	s.md.pollers.heap = []*waitingPoller{queryOnlyPoller}
	s.True(s.md.pollers.allQueryOnly())

	s.md.pollers.Add(regularPoller)
	s.False(s.md.pollers.allQueryOnly())
	s.md.pollers.Remove(regularPoller)
	s.True(s.md.pollers.allQueryOnly())
	s.md.pollers.Remove(queryOnlyPoller)
	s.False(s.md.pollers.allQueryOnly())
}
