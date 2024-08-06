use super::*;

#[test]
fn batch_ordering() {
  let mut batch = Batch::new();

  let k1 = Bytes::from_static(b"bc");
  let k2 = Bytes::from_static(b"abcdefg");
  let k3 = Bytes::from_static(b"abc");
  let k4 = Bytes::from_static(b"abcd");
  let k5 = Bytes::from_static(b"cde");
  let k6 = Bytes::from_static(b"hg");

  batch.push_insert_operation(k1.clone(), Bytes::from_static(b"val2"));
  batch.push_insert_operation(k2.clone(), Bytes::from_static(b"val1"));
  batch.push_insert_operation(k3.clone(), Bytes::from_static(b"val3"));
  batch.push_insert_operation(k4.clone(), Bytes::from_static(b"val4"));
  batch.push_insert_operation(k5.clone(), Bytes::from_static(b"val5"));
  batch.push_insert_operation(k6.clone(), Bytes::from_static(b"val6"));

  let iter = batch.pairs.keys().cloned().map(|k| k.0).collect::<Vec<_>>();

  assert_eq!(iter, vec![k2, k4, k5, k3, k6, k1]);
}
