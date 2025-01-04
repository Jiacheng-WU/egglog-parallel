# Performance TODOs

* Get rid of dashmap for "Predicted Values". This causes some contention when
  running actions. This should "just work" with merge functions but something
  breaks with proofs. Need to look into that.

* Vectorize lookup row calls. This is related to the predicted vals thing, but
  it's a separate optimization/issue.
