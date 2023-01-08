#include <absl/time/civil_time.h>
#include <absl/time/time.h>
#include <absl/log/log.h>
#include <bits/stdc++.h>
#include <fstream>
#include <nlohmann/json.hpp>
template<typename T>
class A{
public:
  T data;
  A(){
    if(std::is_integral_v<T>){
      std::cout << "this is int" << std::endl;
    }else{
      std::cout << "this is else" << std::endl;
    }
  }
};


class DataFrameWithType{
public:
    std::vector<std::string> FrameIndexName;
    std::unordered_map<std::string,std::any> FrameIndex;
public:
    template <typename VAL>
    A<VAL> col(std::string key){
        return std::any_cast<A<VAL>>(FrameIndex[key]);
    }

    template<typename T>
    void operator[](T const&){
        std::cout << "Unaccept input" << std::endl;
    }
};
// template<>
// void DataFrameWithType::operator[]<std::string>(std::string const& s){
//     if(s == "int"){
//       using type = int;
//       return std::any_cast<A<type>>(FrameIndex[s]);
//     }else{
//       using type = double;
//       return std::any_cast<A<type>>(FrameIndex[s]);
//     }
    
// }

class PlaceHolder{
private:
  std::valarray<bool> _place;
public:
  PlaceHolder() = delete;
  PlaceHolder(int n){
    std::cout << "Constructor" << std::endl;
    _place.resize(n);
  }
  PlaceHolder(const PlaceHolder& p){
    std::cout << "Copy Constructor" << std::endl;
    _place = p._place;
  }
  PlaceHolder(PlaceHolder&& p){
    std::cout << "Move Constructor" << std::endl;
    _place = std::move(p._place);
  }
  PlaceHolder operator&& (PlaceHolder& p){
    PlaceHolder result(p._place.size());
    return result;
  }
  PlaceHolder operator&& (PlaceHolder&& p){
    PlaceHolder result(p._place.size());
    return result;
  }
  PlaceHolder operator|| (PlaceHolder& p){
    PlaceHolder result(p._place.size());
    return result;
  }
  PlaceHolder operator|| (PlaceHolder&& p){
    PlaceHolder result(p._place.size());
    return result;
  }
};
template<>
void DataFrameWithType::operator[]<PlaceHolder>(PlaceHolder const& p){
  std::cout << "get holder" << std::endl;
}


int main()
{
  using json = nlohmann::json;
  std::ifstream f("../temp.json");
  json data = json::parse(f);
  std::string type_def = data["pi"];
  if(type_def == "int"){
    using target = int;
      A<target> a;
  }else{
    using target = double;
      A<target> a;
  }
  constexpr absl::Duration ten_ns = absl::Nanoseconds(10);
  constexpr absl::Duration min = absl::Minutes(1);
  constexpr absl::Duration hour = absl::Hours(1);
  absl::CivilSecond ss(2015, 2, 3, 4, 5, 6);  // 2015-02-03 04:05:06
  absl::CivilMinute mm(ss);                   // 2015-02-03 04:05:00
  //std::cout <<  ss + hour*10;
  // std::cout << sizeof(22) << "===" << sizeof(mm);
  DataFrameWithType d;
  //first time Constructor A
  // A&&A, need make result and return a PlaceHolder&& tmp
  d[PlaceHolder(10) && PlaceHolder(5) || PlaceHolder(3)];
  //tmp || A, need result 
  return 0;
}