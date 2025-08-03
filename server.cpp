#include"master.hpp"

int main(){
    Coordinator coord(10,2000);
    coord.handleLoop();
    return 0;
}