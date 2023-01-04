@startuml
class Col<T>{
- vector<T> values
+ Col<T>(std::vector<T>&& data)
+ Col<T>(std::vector<T>& data)
+ Col<T>(Col<T>&& _c)
+ Col<T>(const Col<T>& _c)
+ Col<After> apply(std::function< After(T&)> functor)
+ Col<T> operator+(const Col<T>& _c)
+ Col<T> operator*(const Col<T>& _c)
+ bool operator==(const Col<T>& _c)
}

class DataFrame{
- vector<string> IndexName
- unordered_map<string,any> 
  FrameIndex
+  Col<VAL> col(std::string key)
+  
}

DataFrame o-- Col

class PlaceHolder{
- shared_ptr<DateFrame> df
- vector<int> Selector
}

note left of PlaceHolder::df
    Pointer to the DataFrame which 
    create the PlaceHolder
end note

DataFrame *-- PlaceHolder

class Grouper{

}
@enduml