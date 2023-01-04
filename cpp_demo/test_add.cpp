#include <bits/stdc++.h>

class TimeFunctor{
public:
    static const int x = 10;
    double operator()(int& x){
        return static_cast<double>(x)*2;
    }
};

template<class T>
class Col{
public:
    mutable std::mutex _date_locker;
    std::vector<T> values;
    Col<T>() = delete;
    Col<T>(std::vector<T>&& data):values(std::move(data)){};
    Col<T>(std::vector<T>& data):values(data){};
    Col<T>(Col<T>&& _c){
        std::cout << " using && cons" << std::endl;
        values = std::move(_c.values);
        std::cout << " data move" << std::endl;
    }
    Col<T>(const Col<T>& _c){
        values = _c.values;
    }
        
    template<typename D, 
            std::enable_if_t<std::is_same_v<int,D>, int> = 0 >
    Col<D> _deep_copy() {
        //get date deep_copy of self
        std::cout << " using copy of int" << std::endl;
        return Col<D>(values);
    }

    //RVO, no need move
    template<class After>
    Col<After> apply(std::function< After(T&)> functor){
        std::cout << " using apply" << std::endl;
        std::vector<After> new_col;
        for(auto& x : values){
            new_col.emplace_back(functor(x));
        }
        return Col<After>(new_col);
    }

    Col<T> operator+(const Col<T>& _c){
        std::cout << "here" << std::endl;
        return Col<T>(values);
    }

    Col<T> operator*(const Col<T>& _c){
        std::cout << "here" << std::endl;
        return Col<T>(values);
    }
    void operator==(const T){
        std::cout << "here" << std::endl;
        return;
    }

};



class DataFrameWithType{
public:
    std::vector<std::string> FrameIndexName;
    std::unordered_map<std::string,std::any> FrameIndex;
public:
    template <typename VAL>
    Col<VAL> col(std::string key){
        return std::any_cast<Col<VAL>>(FrameIndex[key]);
    }

    
};

int main()
{
    std::vector<int> tmp{1,2,3,4,5};
    Col<int> int_c(std::move(tmp)); 
    int_c._deep_copy<int>();
    TimeFunctor tf;
    auto func = tf;
    Col<double> double_c(int_c.apply<double>(tf));
    DataFrameWithType d_test;
    d_test.FrameIndex["int_c"] = int_c;
    auto test_add1 = int_c + int_c;
    auto test_add2 = double_c + int_c.apply<double>(tf);
    //aa_B = aa.loc[aa.bs == 'B']
    return 0;
}