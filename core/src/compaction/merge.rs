/// A key-value pair where `None` value represents a tombstone.
pub(crate) type KvPair = (Vec<u8>, Option<Vec<u8>>);

/// K-way merge of multiple sorted SSTable entry lists.
///
/// Each input is a `Vec<(key, Option<value>)>` from one SSTable, sorted by
/// key. Later inputs (higher index) are treated as **newer** and win on
/// duplicate keys.
///
/// Returns a single merged, sorted, deduplicated sequence.
pub(crate) fn k_way_merge(inputs: Vec<Vec<KvPair>>) -> Vec<KvPair> {
    if inputs.is_empty() {
        return Vec::new();
    }
    if inputs.len() == 1 {
        return inputs.into_iter().next().unwrap();
    }

    // Cursors: one per input, tracking the current position.
    let mut cursors: Vec<std::iter::Peekable<std::vec::IntoIter<KvPair>>> = inputs
            .into_iter()
            .map(|v| v.into_iter().peekable())
            .collect();

    let mut result = Vec::new();

    loop {
        // Find the smallest key across all cursors.
        let mut min_key: Option<&[u8]> = None;
        for cursor in &mut cursors {
            if let Some((k, _)) = cursor.peek() {
                match min_key {
                    None => min_key = Some(k.as_slice()),
                    Some(current_min) if k.as_slice() < current_min => {
                        min_key = Some(k.as_slice());
                    }
                    _ => {}
                }
            }
        }

        let min_key = match min_key {
            Some(k) => k.to_vec(),
            None => break, // all cursors exhausted
        };

        // Collect entries matching min_key from all cursors.
        // The last one (highest index = newest) wins.
        let mut winner: Option<(Vec<u8>, Option<Vec<u8>>)> = None;
        for cursor in &mut cursors {
            if let Some((k, _)) = cursor.peek() {
                if k.as_slice() == min_key.as_slice() {
                    winner = cursor.next();
                }
            }
        }

        if let Some(entry) = winner {
            result.push(entry);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv(key: &str, val: &str) -> (Vec<u8>, Option<Vec<u8>>) {
        (key.as_bytes().to_vec(), Some(val.as_bytes().to_vec()))
    }

    fn tomb(key: &str) -> (Vec<u8>, Option<Vec<u8>>) {
        (key.as_bytes().to_vec(), None)
    }

    #[test]
    fn merge_empty_inputs() {
        assert!(k_way_merge(vec![]).is_empty());
    }

    #[test]
    fn merge_single_input() {
        let input = vec![kv("a", "1"), kv("b", "2")];
        let result = k_way_merge(vec![input.clone()]);
        assert_eq!(result, input);
    }

    #[test]
    fn merge_disjoint() {
        let a = vec![kv("a", "1"), kv("c", "3")];
        let b = vec![kv("b", "2"), kv("d", "4")];
        let result = k_way_merge(vec![a, b]);
        assert_eq!(
            result,
            vec![kv("a", "1"), kv("b", "2"), kv("c", "3"), kv("d", "4")]
        );
    }

    #[test]
    fn merge_duplicates_newer_wins() {
        // b (index 1) is newer than a (index 0).
        let a = vec![kv("key", "old")];
        let b = vec![kv("key", "new")];
        let result = k_way_merge(vec![a, b]);
        assert_eq!(result, vec![kv("key", "new")]);
    }

    #[test]
    fn merge_tombstone_wins() {
        let a = vec![kv("key", "alive")];
        let b = vec![tomb("key")];
        let result = k_way_merge(vec![a, b]);
        assert_eq!(result, vec![tomb("key")]);
    }

    #[test]
    fn merge_three_way() {
        let a = vec![kv("a", "1"), kv("c", "old-c")];
        let b = vec![kv("b", "2"), kv("c", "mid-c")];
        let c = vec![kv("c", "new-c"), kv("d", "4")];
        let result = k_way_merge(vec![a, b, c]);
        assert_eq!(
            result,
            vec![kv("a", "1"), kv("b", "2"), kv("c", "new-c"), kv("d", "4")]
        );
    }
}
