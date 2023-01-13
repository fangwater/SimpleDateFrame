#include <bits/stdc++.h>
#include <execution>
#include <absl/time/civil_time.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_split.h>
#include <absl/strings/numbers.h>
#include <absl/log/log.h>

class PlaceHolder{
public:
  std::vector<bool> _place;
public:
  PlaceHolder() = delete;
  PlaceHolder(std::vector<bool> p){
    std::cout << "Constructor by vector<bool>" << std::endl;
    _place = p;
  }
  PlaceHolder(const PlaceHolder& p){
    std::cout << "Copy Constructor" << std::endl;
    _place = p._place;
  }
  PlaceHolder(PlaceHolder&& p){
    std::cout << "Move Constructor" << std::endl;
    _place = p._place;
  }
  PlaceHolder operator&& (PlaceHolder& p){
    std::vector<bool> result(p._place.size());
        std::transform(\
        std::execution::par,p._place.cbegin(),p._place.cend(),_place.cbegin(),result.begin(),\
        [](bool a,bool b){ return a&&b; });
    return PlaceHolder(result);
  }
  PlaceHolder operator&& (PlaceHolder&& p){
    std::vector<bool> result(p._place.size());
        std::transform(\
        std::execution::par,p._place.cbegin(),p._place.cend(),_place.cbegin(),result.begin(),\
        [](bool a,bool b){ return a&&b; });
    return PlaceHolder(result);
  }
  PlaceHolder operator|| (PlaceHolder& p){
    std::vector<bool> result(p._place.size());
        std::transform(\
        std::execution::par,p._place.cbegin(),p._place.cend(),_place.cbegin(),result.begin(),\
        [](bool a,bool b){ return a||b; });
    return PlaceHolder(result);
  }
  PlaceHolder operator|| (PlaceHolder&& p){
    std::vector<bool> result(p._place.size());
        std::transform(\
        std::execution::par,p._place.cbegin(),p._place.cend(),_place.cbegin(),result.begin(),\
        [](bool a,bool b){ return a||b; });
    return PlaceHolder(result);
  }
};

template<class T>
class Col{
public:
    std::vector<T> values;
    Col<T>(){};
    Col<T>(std::vector<T>&& data):values(std::move(data)){};
    Col<T>(std::vector<T>& data):values(data){};
    Col<T>(Col<T>&& _c){
        std::cout << "using move Constructor" << std::endl;
        values = _c.values;
    }
    Col<T>(Col<T>& _c){
        values = _c.values;
    }

    Col<T>& operator=(const Col<T>& _c){
      values = _c.values;
      std::cout << "using copy assign" << std::endl;
      return *this;
    } 

    template<typename D, 
            std::enable_if_t<std::is_same_v<int,D>, int> = 0 >
    Col<D> _deep_copy() {
        //get date deep_copy of self
        std::cout << " using copy of int" << std::endl;
        return Col<D>(values);
    }

    //RVO, no need move
    template<class After,
        std::enable_if_t<std::is_same_v<T,After>, int> = 0>
    Col<After> apply(std::function< After(T&)> functor){
        std::cout << " using apply" << std::endl;
        std::vector<After> new_col;
        for(auto& x : values){
            new_col.emplace_back(functor(x));
        }
        return Col<After>(new_col);
    }

    std::pair<Col<T>,Col<int>> count(){
        //iter for count
        std::unordered_map<T,int> count_map; 
        for(int i = 0; i < values.size(); i++){
          if(count_map.find(values[i]) != count_map.end()){
            count_map[values[i]]++;
          }else{
            count_map[values[i]] = 1;
          }
        }
        std::vector<T> result;
        std::vector<int> counts;
        for(auto iter = count_map.begin(); iter != count_map.end(); iter++){
          result.emplace_back(iter->first);
          counts.emplace_back(iter->second);
        }
        return std::make_pair<Col<T>,Col<int>>(\
          Col<T>(result),Col<int>(counts)
        );
    }

    Col<T> operator-(const Col<T>& _c){
        std::vector<T> result(_c.values.size());
            std::transform(
            std::execution::unseq,\
            values.cbegin(),values.cend(),_c.values.cbegin(),result.begin(),\
            [](T a,T b){ return a-b; });
        return Col<T>(result);
    }

    Col<T> operator+(const Col<T>& _c){
        std::vector<T> result(_c.values.size());
            std::transform(
            std::execution::unseq,\
            values.cbegin(),values.cend(),_c.values.cbegin(),result.begin(),\
            [](T a,T b){ return a+b; });
        return Col<T>(result);
    }

    Col<T> operator*(const Col<T>& _c){
        std::vector<T> result(_c.values.size());
        std::transform(
            std::execution::unseq,\
            values.cbegin(),values.cend(),_c.values.cbegin(),result.begin(),\
            [](T a,T b){ return a*b; });
        return Col<T>(result);
    }

    PlaceHolder operator==(const T obj){
        int n = values.size();
        std::vector<bool> determine(n);
        for(int i = 0; i < n; ++i){
            determine[i] = (values[i] == obj) ? true : false;
        }
        return PlaceHolder(determine);
    }

    PlaceHolder operator>(const T obj){
        int n = values.size();
        std::vector<bool> determine(n);
        for(int i = 0; i < n; ++i){
            determine[i] = (values[i] > obj) ? true : false;
        }
        return PlaceHolder(determine);
    }
    PlaceHolder operator<(const T obj){
        int n = values.size();
        std::vector<bool> determine(n);
        for(int i = 0; i < n; ++i){
            determine[i] = (values[i] < obj) ? true : false;
        }
        return PlaceHolder(determine);
    }
};

template<>
Col<std::string> Col<std::string>::operator+(const Col<std::string>& _c){
    std::vector<std::string> result(_c.values.size());
    std::cout << "partial specialization for string" << std::endl; 
    std::transform(
    std::execution::unseq,\
    values.cbegin(),values.cend(),_c.values.cbegin(),result.begin(),\
    [](std::string a,std::string b){ return a+b; });
    return Col<std::string>(result);
}

enum class Basic_type{
    string,
    int32,
    int64,
    double64,
    datatime
};

template<typename T>
Col<T> select_col(Col<T> col, const PlaceHolder p){
    std::vector<T> result;
    for(int i = 0; i < p._place.size(); i++){
        if( p._place[i] ){
            result.emplace_back(col.values[i]);
        }
    }
    return Col<T>(result);
}


using col_var = std::variant<Col<int>,Col<double>,Col<std::string>>;

class DataFrame{
public:
    std::vector<std::string> keys;
    std::unordered_map<std::string,Basic_type> ColType;
    std::unordered_map<std::string,col_var> FrameIndex;
    using select_args = std::tuple<std::vector<std::string>,std::vector<Basic_type>,PlaceHolder>;
public:
    template <typename VAL>
    Col<VAL> col(std::string key){
        return std::get<Col<VAL>>(FrameIndex[key]);
    };
    // col_var& operator[](std::string key){};
    DataFrame operator[](PlaceHolder const p);
    DataFrame operator[](select_args s_args);
    col_var& operator()(std::string key, Basic_type col_type);
};

col_var& DataFrame::operator()(std::string key, Basic_type col_type){
    //find key from map
    auto iter = FrameIndex.find(key);
    if(iter != FrameIndex.end()){
      //find, return a ref of std::any
      return iter->second;
    }else{
      // //not find, add first
      FrameIndex.insert(std::pair<std::string,col_var>(key,col_var()));
      //add key
      keys.emplace_back(key);
      //register type info to dataframe
      ColType[key] = col_type;
      //return ref to get value
      return FrameIndex[key];
    }
}


DataFrame DataFrame::operator[](PlaceHolder p){
  DataFrame selected;
  std::vector<std::future<col_var>> res;
  for(auto key : keys){
    if(ColType[key] == Basic_type::int32){
      std::future<col_var> x = std::async(&select_col<int>,this->col<int>(key),p);
    }
    // }else if(ColType[key] == Basic_type::double64){
    //   res.emplace_back(std::async(&select_col<double>,this->col<double>(key),p));
    // }else if(ColType[key] == Basic_type::string){
    //   res.emplace_back(std::async(&select_col<std::string>,this->col<std::string>(key),p));
    // }
  }
  for(int i = 0; i < keys.size(); i++){
    selected(keys[i],ColType[keys[i]]) = res[i].get();
  }
  return selected;
}


template<typename T>
void show_v(std::vector<T> vec){
    for(auto v: vec){
        std::cout << v << " "; 
    }
    std::cout << std::endl;
}

enum class Freq{
T,
H,
D
};
enum class Close{
  right,
  left
};

enum class Label{
  right,
  left
};

template<int period , Freq F>
class Interval{
  using SecT = absl::CivilSecond;
public:
  constexpr static int __interval(){
  if(F == Freq::T){
    return 60 * period;
  }else if(F == Freq::H){
    return 60 * 60 * period;
  }else if(F == Freq::D){
    return 24 * 60 * 60 * period;
  }else{
    return 1;
    }
  }

  inline SecT FloorNorm(SecT time){
      if(F == Freq::T){
        absl::CivilMinute mm(time);
        return absl::CivilSecond(mm);  
      }
      else if(F == Freq::H){
        absl::CivilHour hh(time);
        return absl::CivilSecond(hh);
      }
      else if(F == Freq::D){
        absl::CivilDay dd(time);
        return absl::CivilSecond(dd); 
      }
      else{
        LOG(FATAL) << "Unaccept type of freq precision";
      }
  }
  inline SecT CeilNorm(SecT time){
      if(F == Freq::T){
        absl::CivilMinute mm(time + 60);
        return absl::CivilSecond(mm);  
      }
      else if(F == Freq::H){
        absl::CivilHour hh(time + 60*60);
        return absl::CivilSecond(hh);
      }
      else if(F == Freq::D){
        absl::CivilDay dd(time + 60*60*24);
        return absl::CivilSecond(dd); 
      }
      else{
        LOG(FATAL) << "Unaccept type of freq precision";
      }
  }
};

//if value
class GroupByValue{
public:
  //Grouped[row_id] = Group_id
  std::vector<int> Grouped;
  //Group_value[Group_id] = value
  std::unordered_map<int,std::any> Group_value;
};
//if time

class GroupByInterval{
public:
  //Grouped[row_id] = Group_id;
  std::vector<int> Grouped;
  absl::CivilSecond start;
  absl::CivilSecond interval;
  Label direction;
  int size;
};

class GroupByExecutor{
public:
    using GroupResult = std::variant<GroupByValue,GroupByInterval>; 
    using GR_sp_future = std::variant<std::future<GroupResult>>;
    std::shared_ptr<DataFrame> df;
    std::vector<std::future<GroupResult>> res;
    
    // template<class T>
    // GR_sp_future add_grouper(std::string& key){ 
    //   Col<int>& col = df->ref<int>(key);
    //   std::vector<int> groupped(col.values.size());
    //   return GroupByValue();
    // }
    // template<int period, Freq F, Close c, Label l>
    // GR_sp_future add_grouper(std::string& key){
    //   Interval<period,F> Interval;
    //   return GroupByInterval();
    // }
    // bool exec();
    void get_result(){
    }
    void get_join_result(std::vector<int> x){
    }
};



int main()
{
    DataFrame df;
    std::vector<double> tradv{0.1,0.2,0,3};
    std::vector<double> tradp{0.4,0.5,0,6};
    df("tradv",Basic_type::double64) = Col<double>(tradv);
    df("tradp",Basic_type::double64) = Col<double>(tradp);
    auto vv = df.col<double>("tradv");
    df("tradmt",Basic_type::double64) = (df.col<double>("tradv")) * (df.col<double>("tradp"));
    vv = df.col<double>("tradmt").values;
    std::vector<std::string> ss{"aa","bb","cc"};
    std::vector<std::string> tt{"dd","ee","ff"};
    df("test_concat",Basic_type::string) = Col<std::string>(ss) + Col<std::string>(tt);
    show_v<double>(vv.values);
    auto p = df.col<double>("tradv") > 0.1;
    std::cout << "select p: " << std::endl;
    show_v(p._place);
    auto p1 = df.col<double>("tradv") > 0.1 && df.col<double>("tradp") == double(0.5);
    std::cout << "select p1: " << std::endl;
    show_v(p1._place);
    //select only need col is best, but not easy to use
    DataFrame as_B;
    auto fucol = std::async(&select_col<double>,df.col<double>("tradv"),p);
    auto se = fucol.get();
    show_v(se.values);
    std::cout << "count test: " << std::endl;
    auto count_res = df.col<double>("tradv").count();
    show_v(count_res.first.values);
    show_v(count_res.second.values);
    //select all is also impl
    DataFrame selected = df[p];
    return 0;
}
