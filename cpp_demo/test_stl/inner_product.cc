#include <numeric>
#include <iostream>
#include <vector>
#include <functional>
#include <execution>
int main()
{
    std::vector<int> a{1, 1, 1};
    std::vector<int> b{2, 2, 2};
 
    int r1 = std::inner_product(a.begin(), a.end(), b.begin(), 1);
    std::cout << "Inner product of a and b: " << r1 << '\n';
 
    int r2 = std::inner_product(std::execution::par_unseq,a.begin(), a.end(), b.begin(), -1,
                                std::plus<>(), std::equal_to<>());
    std::cout << "Number of pairwise matches between a and b: " <<  r2 << '\n';
}