@startuml
class Col<T>{
+ Col<After> apply(std::function< After(T&)> functor)
+ std::pair<Col<T>,Col<int>> count()
+ Col<T> operator+-*(const Col<T>& _c)
+ bool operator==><(const Col<T>& _c)
}

class DataFrame{
- vector<string> IndexName
- unordered_map<string,any> FrameIndex
+ Col<VAL> col(std::string key)
+ Col<VAL>& ref(std::string key)
+ std::any& operator[](std::string const& key)
}

DataFrame o-- Col

class PlaceHolder{
- std::vector<bool> _place
- PlaceHolder operator&&|| (PlaceHolder& p)
- PlaceHolder operator&&|| (PlaceHolder&& p)
}

note left of PlaceHolder::operator&&|
    [1] PlaceHolder is just an intermediate representation for expression
     for which rows are selected by an expression.
    [2] Reduce copying and operations
end note

Col *-- PlaceHolder

class Date{
- absl::CivilSecond civil_time
- short bias_ms
}
Col *-- Date

class Interval<int period , freq F>{
- using SecT = absl::CivilSecond
+ constexpr static int __interval()
+ SecT FloorNorm(SecT time)
+ SecT CeilNorm(SecT time)
}

Interval *-- Date

class GroupByValue{
- vector<int> Grouped
- unordered_map<int,std::any> Group_value
}
class GroupByValue{
- std::vector<int> Grouped;
- absl::CivilSecond start;
- absl::CivilSecond interval;
- label direction;
- int size;
}


class GroupByExecutor{

}
GroupByExecutor *-- Interval
GroupByExecutor *-- GroupByValue
enum freq{
T,
H,
D
}

Interval *-- freq

enum close{
  right,
  left
}
GroupByExecutor *-- close

enum label{
  right,
  left
}
GroupByExecutor *-- label
@enduml

