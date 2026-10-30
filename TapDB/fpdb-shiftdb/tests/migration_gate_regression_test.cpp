#include "shiftdb/migration_gate.hpp"
#include <iostream>
#include <stdexcept>
using shiftdb::MigrationGate;
void check(bool x){if(!x)throw std::runtime_error("check failed");}
template<class F>void rejects(F f){bool rejected=false;try{f();}catch(const std::exception&){rejected=true;}check(rejected);}
int main(){
 MigrationGate source,target;
 rejects([&]{source.freeze("");});
 check(source.freeze("m1"));check(!source.freeze("m1"));
 rejects([&]{source.freeze("m2");});
 rejects([&]{source.commit("wrong","node2");});
 check(target.prepare("m1",2));check(!target.prepare("m1",2));
 rejects([&]{target.prepare("m1",3);});
 check(source.commit("m1","node2"));check(!source.commit("m1","node2"));
 rejects([&]{source.commit("m1","node3");});
 rejects([&]{source.abort_source("m1");});
 rejects([&]{source.freeze("m2");});
 check(target.activate("m1"));check(!target.activate("m1"));
 rejects([&]{target.abort_target("m1");});
 rejects([&]{target.activate("wrong");});
 check(source.phase()==MigrationGate::Phase::Forwarding);
 check(target.phase()==MigrationGate::Phase::Active);
 MigrationGate cancel;
 cancel.freeze("cancel");cancel.abort_source("cancel");
 check(cancel.phase()==MigrationGate::Phase::Running);
 rejects([&]{cancel.commit("cancel","node2");});
 check(cancel.freeze("new"));
 MigrationGate discard;discard.prepare("discard",2);discard.abort_target("discard");
 rejects([&]{discard.activate("discard");});
 rejects([&]{discard.prepare("new",2);});
 std::cout<<"Migration ticket, retry, cutover fencing and abort checks passed\n";
}
