#include <bits/stdc++.h>
#include <execution>
class A{
public:
    template<typename Y>void operator[](Y const&){
        std::cout << "General" << std::endl;
    }
};

template<typename X>
class B{
public:
    template<typename Y>
    void operator[](Y const&){
        std::cout << "General" << std::endl;
    }
};


template<>
void A::operator[]<int>(int const&){
    std::cout << "int" << std::endl;
}


// class place_holder{

// };

// class Mapper{
// public:
//     static const int x = 10;
//     double operator()(int& x){
//         return static_cast<double>(x)*2;
//     }
// };

// class TimeSeriesMapper{
// public:
//     int period;
//     template<typename T>
//     int operator()(const T&)
// };

// class Grouper{
//     //map[row_id] = group_id
//     std::multimap<int,int> Grouper_mapping;
//     static place_holder get_ans(){
//         return place_holder();
//     };
//     template<typename T>
    

    
// };

inline bool apply_and(bool a, bool b){
    return a && b;
}

int main()
{
    int a[] = {0,1};
    std::vector<int> v(2);
    std::for_each(std::execution::par, std::begin(a), std::end(a), [&](int i) {
        v[i] = i+100; 
    });
    std::vector<bool> v1{0,1,1,1,1,1};
    std::vector<bool> v2{1,1,1,1,1,0};
    auto result = std::vector<bool>(v1.size());
    std::transform(std::execution::par_unseq,v1.begin(),v1.end(),v2.begin(),result.begin(),[](bool a,bool b){ return a&&b; });
    std::cout << std::boolalpha;
    for(auto x: result){
        std::cout << x << std::endl;
    }
    return 0;
}