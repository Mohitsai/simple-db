#include <db/ColumnStats.hpp>
#include <stdexcept>

using namespace db;

ColumnStats::ColumnStats(unsigned buckets, int min, int max):
    bucketCount(buckets), min(min), max(max),
    bucketWidth((buckets + (max - min)) / buckets),
    histogram(buckets, 0), tupleCount(0) {}

// TODO: Add Value Implementation
void ColumnStats::addValue(int v) {
    if (!(min <= v && v <= max)) {
      throw std::out_of_range("value not in range of the histogram.");
    }
    histogram[(v - min) / bucketWidth]++;
    tupleCount++;
}

// TODO: Estimate Cardinality Implementation
size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const {
    if (tupleCount == 0) {
      return 0;
    }

    size_t cardinality = 0;
    int index = -1;
    if (min <= v && v <= max) {index = (v - min) / bucketWidth;}

    switch (op) {
    case PredicateOp::EQ:
      if (index != -1) { cardinality = histogram[index] / bucketWidth; }
      break;

    case PredicateOp::GT:
      if (v < min) { return tupleCount; }
      if (max <= v) { return 0; }
      if (index != -1) {
        int right = min + (index + 1) * bucketWidth - 1;
        cardinality += ((right - v) * histogram[index]) / bucketWidth;
        for (size_t i = index + 1; i < histogram.size(); ++i) {
          cardinality += histogram[i];
        }
      }
      break;

    case PredicateOp::GE:
      if (v < min) { return tupleCount; }
      if (v > max) { return 0; }
      if (index != -1) {
        int right = min + (index + 1) * bucketWidth - 1;
        cardinality += ((right - v + 1) * histogram[index]) / bucketWidth;
        for (size_t i = index + 1; i < histogram.size(); ++i) { cardinality += histogram[i]; }
      }
      break;

    case PredicateOp::LT:
      if (v <= min) { return 0;}
      if (v > max) { return tupleCount; }
      if (index != -1) {
        int left = min + index * bucketWidth;
        cardinality += ((v - left) * histogram[index]) / bucketWidth;
        for (int i = index - 1; i >= 0; --i) { cardinality += histogram[i]; }
      }
      break;

    case PredicateOp::LE:
      return estimateCardinality(PredicateOp::EQ, v) + estimateCardinality(PredicateOp::LT, v);

    case PredicateOp::NE:
      return tupleCount - estimateCardinality(PredicateOp::EQ, v);
    }

    return cardinality;
}
