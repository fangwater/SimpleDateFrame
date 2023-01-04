#include <bits/stdc++.h>
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


int main()
{
    A a;
    a["123"];
    B<int> b;
    b<double>["123"];
    return 0;
}