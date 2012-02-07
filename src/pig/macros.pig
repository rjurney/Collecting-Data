/* Get a count of records, return the name of the relation and . */
DEFINE total_count(relation) RETURNS total {
  $total = FOREACH (group $relation all) generate '$relation' as label, COUNT_STAR($relation) as total;
};

/* Get totals on 2 relations, union and return them with labels */
DEFINE compare_totals(r1, r2) RETURNS totals {
  total1 = total_count($r1);
  total2 = total_count($r2);
  $totals = union total1, total2;
};

/* See how many records from a relation are removed by a filter, given a condition */
DEFINE test_filter(original, condition) RETURNS result {
  filtered = filter $original by $condition;
  $result = compare_totals($original, filtered);
};

