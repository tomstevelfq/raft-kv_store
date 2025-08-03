#include"master.hpp"

int main(){
    Coordinator coord(10,300);
    coord.handleLoop();
    return 0;
}