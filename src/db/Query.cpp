#include <map>
#include <unordered_set>
#include <db/Query.hpp>
#include <stdexcept>

using namespace db;

void db::projection(const DbFile &in, DbFile &out, const std::vector<std::string> &field_names) {
  // TODO: Implement this function
  const TupleDesc &in_tuple_desc = in.getTupleDesc();
  std::vector<type_t> out_types;
  std::vector<size_t> field_indices;
  std::vector<std::string> unique_field_names;

  for (size_t i = 0; i < field_names.size(); ++i) {
    size_t index = in_tuple_desc.index_of(field_names[i]);
    field_indices.push_back(index);

    auto it = in.begin();
    Tuple sampleTuple = in.getTuple(it);
    out_types.push_back(sampleTuple.field_type(index));

    std::string name = field_names[i] + "_" + std::to_string(i);
    unique_field_names.push_back(name);
  }

  TupleDesc out_tuple_desc(out_types, unique_field_names);

  for (auto it = in.begin(); it != in.end(); in.next(it)) {
    Tuple inTuple = in.getTuple(it);
    std::vector<field_t> out_fields;

    out_fields.reserve(field_indices.size());
    for (size_t index : field_indices) {
      out_fields.push_back(inTuple.get_field(index));
    }

    out.insertTuple(Tuple(out_fields));
  }
}

void db::filter(const DbFile &in, DbFile &out, const std::vector<FilterPredicate> &pred) {
  // TODO: Implement this function
  for (auto it = in.begin(); it != in.end(); in.next(it)) {
    Tuple tuple = in.getTuple(it);
    bool satisfy_predicate = true;

    const TupleDesc& tuple_desc = in.getTupleDesc();

    for (const auto &predicate : pred) {
      const size_t field_index = tuple_desc.index_of(predicate.field_name);
      const field_t& field_value = tuple.get_field(field_index);

      switch (predicate.op) {
        case PredicateOp::EQ:
          satisfy_predicate &= (field_value == predicate.value);
        break;
        case PredicateOp::NE:
          satisfy_predicate &= (field_value != predicate.value);
        break;
        case PredicateOp::LT:
          satisfy_predicate &= (field_value < predicate.value);
        break;
        case PredicateOp::LE:
          satisfy_predicate &= (field_value <= predicate.value);
        break;
        case PredicateOp::GT:
          satisfy_predicate &= (field_value > predicate.value);
        break;
        case PredicateOp::GE:
          satisfy_predicate &= (field_value >= predicate.value);
        break;
      }

      if (!satisfy_predicate) break;
    }

    if (satisfy_predicate) {
      out.insertTuple(tuple);
    }
  }
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
    // TODO: Implement this function
    const TupleDesc &in_tuple_desc = in.getTupleDesc();
    size_t agg_field_index = in_tuple_desc.index_of(agg.field);
    size_t group_field_index;
    bool has_group = agg.group.has_value();

    if (has_group) {
        group_field_index = in_tuple_desc.index_of(agg.group.value());
    }

    Iterator it = in.begin();
    if (it == in.end()) {
        throw std::logic_error("Input DbFile is empty");
    }
    Tuple temp_tuple = in.getTuple(it);

    std::vector<type_t> out_types;
    std::vector<std::string> out_field_names;

    if (has_group) {
        out_types.push_back(temp_tuple.field_type(group_field_index));
        out_field_names.push_back(agg.group.value());
    }

    type_t agg_type;
    if (agg.op == AggregateOp::AVG) {
        agg_type = type_t::DOUBLE;
    } else {
        agg_type = temp_tuple.field_type(agg_field_index);
    }
    out_types.push_back(agg_type);
    std::string agg_field_name;
    switch (agg.op) {
        case AggregateOp::COUNT:
            agg_field_name = "count";
            break;
        case AggregateOp::SUM:
            agg_field_name = "sum";
            break;
        case AggregateOp::AVG:
            agg_field_name = "avg";
            break;
        case AggregateOp::MIN:
            agg_field_name = "min";
            break;
        case AggregateOp::MAX:
            agg_field_name = "max";
            break;
        default:
            throw std::logic_error("Unsupported operation type for aggregation");
    }
    out_field_names.push_back(agg_field_name);

    struct AggData {
        int count = 0;
        field_t sum;
        field_t min;
        field_t max;
        bool is_initialized = false;

        void update(const field_t &value) {
            count++;
            if (!is_initialized) {
                sum = value;
                min = value;
                max = value;
                is_initialized = true;
            } else {
                if (std::holds_alternative<int>(value)) {
                    sum = std::get<int>(sum) + std::get<int>(value);
                } else if (std::holds_alternative<double>(value)) {
                    sum = std::get<double>(sum) + std::get<double>(value);
                } else {
                    throw std::logic_error("Unsupported field type for aggregation");
                }

                if (value < min) min = value;
                if (value > max) max = value;
            }
        }

        field_t getResult(AggregateOp op) const {
            switch (op) {
                case AggregateOp::COUNT:
                    return count;
                case AggregateOp::SUM:
                    return sum;
                case AggregateOp::AVG:
                    if (std::holds_alternative<int>(sum)) {
                        return static_cast<double>(std::get<int>(sum)) / count;
                    }
                    if (std::holds_alternative<double>(sum)) {
                        return std::get<double>(sum) / count;
                    }
                    {
                        throw std::logic_error("Unsupported field type for aggregation");
                    }
                case AggregateOp::MIN:
                    return min;
                case AggregateOp::MAX:
                    return max;
                default:
                    throw std::logic_error("Unsupported operation type for aggregation");
            }
        }
    };

    std::unordered_map<field_t, AggData> group_agg_data;
    AggData total_agg_data;

    for (auto it = in.begin(); it != in.end(); in.next(it)) {
        Tuple tuple = in.getTuple(it);
        const field_t& agg_field = tuple.get_field(agg_field_index);

        if (has_group) {
            const field_t& group_key = tuple.get_field(group_field_index);
            group_agg_data[group_key].update(agg_field);
        } else {
            total_agg_data.update(agg_field);
        }
    }

    if (has_group) {
        for (const auto &[groupKey, aggData] : group_agg_data) {
            std::vector<field_t> out_fields;
            out_fields.push_back(groupKey);
            out_fields.push_back(aggData.getResult(agg.op));
            Tuple out_tuple(out_fields);
            out.insertTuple(out_tuple);
        }
    } else {
        std::vector<field_t> out_fields;
        out_fields.push_back(total_agg_data.getResult(agg.op));
        Tuple out_tuple(out_fields);
        out.insertTuple(out_tuple);
    }
}



void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
    // TODO: Implement this function
    const TupleDesc &left_tuple_desc = left.getTupleDesc();
    const TupleDesc &right_tuple_desc = right.getTupleDesc();

    size_t left_field_index = left_tuple_desc.index_of(pred.left);
    size_t right_field_index = right_tuple_desc.index_of(pred.right);

    Iterator left_iterator = left.begin();
    if (left_iterator == left.end()) {
        throw std::logic_error("Left DbFile is empty");
    }
    Tuple left_temp_tuple = left.getTuple(left_iterator);

    Iterator right_iterator = right.begin();
    if (right_iterator == right.end()) {
        throw std::logic_error("Right DbFile is empty");
    }
    Tuple right_temp_tuple = right.getTuple(right_iterator);

    std::vector<type_t> out_types;
    std::vector<std::string> out_field_names;

    for (size_t i = 0; i < left_tuple_desc.size(); ++i) {
        out_types.push_back(left_temp_tuple.field_type(i));
        out_field_names.push_back("left_" + std::to_string(i));
    }

    for (size_t i = 0; i < right_tuple_desc.size(); ++i) {
        if (pred.op == PredicateOp::EQ && i == right_field_index) continue;
        out_types.push_back(right_temp_tuple.field_type(i));
        out_field_names.push_back("right_" + std::to_string(i));
    }


    if (pred.op == PredicateOp::EQ) {
        std::unordered_multimap<field_t, Tuple> hash_table;

        for (auto it = left.begin(); it != left.end(); left.next(it)) {
            Tuple left_tuple = left.getTuple(it);
            field_t left_key = left_tuple.get_field(left_field_index);
            hash_table.emplace(left_key, left_tuple);
        }

        for (auto it = right.begin(); it != right.end(); right.next(it)) {
            Tuple right_tuple = right.getTuple(it);
            const field_t& right_key = right_tuple.get_field(right_field_index);

            auto range = hash_table.equal_range(right_key);
            for (auto iter = range.first; iter != range.second; ++iter) {
                Tuple left_tuple = iter->second;
                std::vector<field_t> out_fields;

                out_fields.reserve(left_tuple.size());
                for (size_t i = 0; i < left_tuple.size(); ++i) {
                    out_fields.push_back(left_tuple.get_field(i));
                }
                for (size_t i = 0; i < right_tuple.size(); ++i) {
                    if (i == right_field_index) continue;
                    out_fields.push_back(right_tuple.get_field(i));
                }

                Tuple out_tuple(out_fields);
                out.insertTuple(out_tuple);
            }
        }
    } else {
        for (auto itLeft = left.begin(); itLeft != left.end(); left.next(itLeft)) {
            Tuple left_tuple = left.getTuple(itLeft);
            const field_t& left_field = left_tuple.get_field(left_field_index);

            for (auto itRight = right.begin(); itRight != right.end(); right.next(itRight)) {
                Tuple right_tuple=right.getTuple(itRight);
                const field_t& right_field=right_tuple.get_field(right_field_index);

                bool matches=false;
                switch(pred.op){
                    case PredicateOp::NE:
                        matches=(left_field!=right_field);
                        break;
                    case PredicateOp::LT:
                        matches=(left_field<right_field);
                        break;
                    case PredicateOp::LE:
                        matches=(left_field<=right_field);
                        break;
                    case PredicateOp::GT:
                        matches=(left_field>right_field);
                        break;
                    case PredicateOp::GE:
                        matches=(left_field>=right_field);
                        break;
                    default:
                        throw std::logic_error("Unsupported predicate operation for join");
                }

                if (matches) {
                    std::vector<field_t> out_fields;

                    out_fields.reserve(left_tuple.size());
                    for (size_t i = 0; i < left_tuple.size(); ++i) {
                        out_fields.push_back(left_tuple.get_field(i));
                    }

                    for (size_t i = 0; i < right_tuple.size(); ++i) {
                        out_fields.push_back(right_tuple.get_field(i));
                    }

                    Tuple out_tuple(out_fields);
                    out.insertTuple(out_tuple);
                }
            }
        }
    }
}