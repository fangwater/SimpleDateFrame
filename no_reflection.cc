#include <bits/stdc++.h>

enum class VecType{
    is_int,
    is_double
};

using could_be = std::variant<std::vector<int>, std::vector<double>>;

could_be func(could_be v,VecType vt){
    if(vt == VecType::is_int){
        std::cout << "do process for int" << std::endl;
    }else if(vt == VecType::is_double){
        std::cout << "do process for double" << std::endl;
    }
    return v;
}

using fu_c = std::future<could_be>;
int main()
{
    std::vector<int> a{1,2,3,4,5};
    std::vector<double> b{0.1,0,2,0,3,0.4,0.5};
    std::vector<could_be> need_process;
    std::vector<VecType> type_store{VecType::is_int,VecType::is_double};
    need_process.emplace_back(a);
    need_process.emplace_back(b);
    std::vector<fu_c> future_result;
    for(int i = 0; i < need_process.size(); i++){
        future_result.emplace_back(std::async(&func,need_process[i],type_store[i]));
    }
    for(auto& task : future_result){
        task.get();
    }
    return 0;
}



