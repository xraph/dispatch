package redis

// SetListScanBudgetForTest lowers how many index members one paged list
// call on this store examines, so a test can make a filtered scan stop at
// its budget without writing thousands of rows.
func (s *Store) SetListScanBudgetForTest(n int) { s.scanBudget = n }
