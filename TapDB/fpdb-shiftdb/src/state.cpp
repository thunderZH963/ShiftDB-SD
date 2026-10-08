#include "shiftdb/state.hpp"
#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>
namespace shiftdb {
uint64_t checksum(const std::vector<uint8_t>& b) {
 uint64_t h=14695981039346656037ULL;for(auto c:b){h^=c;h*=1099511628211ULL;}return h;
}
namespace {
struct Writer {
 std::vector<uint8_t> b;
 void u(uint64_t x){for(int i=0;i<8;++i)b.push_back(uint8_t(x>>(8*i)));}
 void i(int64_t x){uint64_t bits;std::memcpy(&bits,&x,8);u(bits);}
 void str(const std::string& s){u(s.size());b.insert(b.end(),s.begin(),s.end());}
 void row(const Row& r){i(r.id);i(r.key);u(r.value.has_value());if(r.value)i(*r.value);}
};
struct Reader {
 const std::vector<uint8_t>& b;size_t at=0;
 uint64_t u(){if(b.size()-at<8)throw std::runtime_error("truncated state");uint64_t x=0;for(int i=0;i<8;++i)x|=uint64_t(b[at++])<<(8*i);return x;}
 int64_t i(){uint64_t bits=u();int64_t x;std::memcpy(&x,&bits,8);return x;}
 uint64_t count(){auto n=u();if(n>b.size()-at)throw std::runtime_error("invalid state count");return n;}
 bool flag(){auto n=u();if(n>1)throw std::runtime_error("invalid boolean");return n;}
 std::string str(){auto n=count();std::string s(b.begin()+at,b.begin()+at+n);at+=n;return s;}
 Row row(){Row r;r.id=i();r.key=i();if(flag())r.value=i();return r;}
};
int64_t add(int64_t a,int64_t b) {
 if((b>0&&a>INT64_MAX-b)||(b<0&&a<INT64_MIN-b))throw std::overflow_error("integer SUM overflow");return a+b;
}
}
StatePartition::StatePartition(Kind k,uint64_t limit,uint64_t pending):max_encoded_bytes_(limit),max_pending_bytes_(pending) {
 if(k!=Kind::Aggregate&&k!=Kind::InnerJoin&&k!=Kind::Sort)throw std::invalid_argument("unsupported codec");
 data_.kind=k;
}
Image StatePartition::encode(const Data& d) {
 Writer w;w.u(0x5348494654303031ULL);w.u(1);w.u(uint64_t(d.kind));w.u(d.sequence);w.u(d.output_cursor);w.u(d.finalized);w.u(d.probe_started);
 w.u(d.input.size());for(auto& [edge,seq]:d.input){w.str(edge);w.u(seq);}
 w.u(d.groups.size());for(auto& [key,g]:d.groups){w.i(key);w.i(g.sum);w.i(g.min);w.i(g.max);w.u(g.count);}
 w.u(d.rows.size());for(auto& r:d.rows)w.row(r);
 w.u(d.matches.size());for(auto& r:d.matches){w.i(r.first);w.i(r.second);}
 auto hash=checksum(w.b);return {std::move(w.b),hash};
}
StatePartition::Data StatePartition::decode(const Image& image) {
 if(checksum(image.bytes)!=image.checksum)throw std::runtime_error("state checksum mismatch");
 Reader r{image.bytes};
 if(r.u()!=0x5348494654303031ULL||r.u()!=1)throw std::runtime_error("state codec mismatch");
 Data d;auto kind=r.u();if(kind<1||kind>3)throw std::runtime_error("unsupported state kind");d.kind=Kind(kind);
 d.sequence=r.u();d.output_cursor=r.u();d.finalized=r.flag();d.probe_started=r.flag();
 auto n=r.count();for(uint64_t j=0;j<n;++j){auto edge=r.str();auto seq=r.u();if(!d.input.emplace(edge,seq).second)throw std::runtime_error("duplicate frontier");}
 n=r.count();for(uint64_t j=0;j<n;++j){auto key=r.i();Aggregate g;g.sum=r.i();g.min=r.i();g.max=r.i();g.count=r.u();if(!d.groups.emplace(key,g).second)throw std::runtime_error("duplicate key");}
 n=r.count();for(uint64_t j=0;j<n;++j)d.rows.push_back(r.row());
 n=r.count();for(uint64_t j=0;j<n;++j){auto a=r.i(),b=r.i();d.matches.emplace_back(a,b);}
 if(r.at!=r.b.size()||d.output_cursor>d.rows.size())throw std::runtime_error("invalid state tail");
 return d;
}
void StatePartition::activate(uint64_t generation) {
 std::lock_guard<std::mutex> l(mutex_);if(!generation||generation<generation_)throw std::invalid_argument("stale generation");generation_=generation;frozen_=false;
}
void StatePartition::pause(){std::lock_guard<std::mutex> l(mutex_);frozen_=true;}
void StatePartition::retire(){std::lock_guard<std::mutex> l(mutex_);auto kind=data_.kind;data_=Data{};data_.kind=kind;log_.clear();pending_.clear();logging_=false;frozen_=true;}
Consume StatePartition::consume(const std::string& edge,uint64_t seq,uint64_t gen,const std::vector<Row>& rows) {
 std::lock_guard<std::mutex> l(mutex_);
 if(frozen_||gen!=generation_)return Consume::Fenced;
 if(edge.empty()||!seq)throw std::invalid_argument("invalid input frontier");
 auto old=data_.input.find(edge);auto frontier=old==data_.input.end()?0:old->second;
 if(seq<=frontier)return Consume::Duplicate;
 if(seq!=frontier+1)return Consume::Gap;
 if(data_.finalized)throw std::logic_error("input after finalization");
 Data next=data_; // stage mutation and frontier together, then publish
 if(next.kind==Kind::Aggregate) {
  for(auto& row:rows){auto& g=next.groups[row.key];if(row.value){auto v=*row.value;g.sum=add(g.sum,v);if(!g.count){g.min=v;g.max=v;}else{g.min=std::min(g.min,v);g.max=std::max(g.max,v);}if(g.count==UINT64_MAX)throw std::overflow_error("COUNT overflow");++g.count;}}
 } else if(next.kind==Kind::InnerJoin) {
  if(edge=="build") {if(next.probe_started)throw std::logic_error("build input after probe phase");next.rows.insert(next.rows.end(),rows.begin(),rows.end());}
  else if(edge=="probe") {next.probe_started=true;for(auto& probe:rows)for(auto& build:next.rows)if(build.key==probe.key)next.matches.emplace_back(probe.id,build.id);}
  else throw std::invalid_argument("join requires build/probe input");
 } else next.rows.insert(next.rows.end(),rows.begin(),rows.end());
 if(next.sequence==UINT64_MAX)throw std::overflow_error("log sequence overflow");
 next.input[edge]=seq;++next.sequence;auto img=encode(next);
 if(img.bytes.size()>max_encoded_bytes_)return Consume::Wait;
 // Reference post-image log: one full partition image per changed batch.
 if(logging_) {
  uint64_t outstanding=0;for(const auto& [seq,d]:log_)outstanding+=d.post_image.bytes.size();
  if(img.bytes.size()>max_pending_bytes_||outstanding>max_pending_bytes_-img.bytes.size())return Consume::Wait;
  log_.emplace(next.sequence,Delta{ticket_,next.sequence,img});
 }
 data_=std::move(next);return Consume::Applied;
}
void StatePartition::finalize(uint64_t gen) {
 std::lock_guard<std::mutex> l(mutex_);
 if(frozen_||gen!=generation_)throw std::logic_error("fenced finalize");
 if(data_.finalized)return;
 auto next=data_;
 if(next.kind==Kind::Sort)std::stable_sort(next.rows.begin(),next.rows.end(),[](const Row& a,const Row& b){return a.key<b.key;});
 next.finalized=true;if(next.sequence==UINT64_MAX)throw std::overflow_error("log sequence overflow");++next.sequence;
 auto img=encode(next);
 if(logging_) {
  uint64_t outstanding=0;for(const auto& [seq,d]:log_)outstanding+=d.post_image.bytes.size();
  if(img.bytes.size()>max_pending_bytes_||outstanding>max_pending_bytes_-img.bytes.size())throw std::runtime_error("delta log budget exhausted; catch up before retry");
  log_.emplace(next.sequence,Delta{ticket_,next.sequence,img});
 }
 data_=std::move(next);
}
Image StatePartition::snapshot(const std::string& ticket) {
 std::lock_guard<std::mutex> l(mutex_);if(ticket.empty()||logging_)throw std::logic_error("active snapshot or empty ticket");
 auto image=encode(data_);ticket_=ticket;log_.clear();logging_=true;return image;
}
void StatePartition::restore(const Image& image,const std::string& ticket) {
 if(image.bytes.size()>max_encoded_bytes_)throw std::runtime_error("state exceeds encoded budget");
 auto next=decode(image);std::lock_guard<std::mutex> l(mutex_);
 if(!frozen_||next.kind!=data_.kind||ticket.empty())throw std::logic_error("invalid restore");
 data_=std::move(next);ticket_=ticket;pending_.clear();log_.clear();logging_=false;
}
uint64_t StatePartition::apply(const Delta& delta) {
 std::lock_guard<std::mutex> l(mutex_);
 if(!frozen_||delta.ticket!=ticket_)throw std::logic_error("invalid replay ticket or active target");
 if(delta.sequence<=data_.sequence)return data_.sequence;
 if(delta.post_image.bytes.size()>max_encoded_bytes_)throw std::runtime_error("oversized delta");
 auto decoded=decode(delta.post_image);
 if(decoded.kind!=data_.kind||decoded.sequence!=delta.sequence)throw std::runtime_error("invalid delta metadata");
 uint64_t queued=0;for(auto& [n,d]:pending_)queued+=d.post_image.bytes.size();
 auto exists=pending_.find(delta.sequence);
 if(exists!=pending_.end()) {
  if(exists->second.post_image.bytes!=delta.post_image.bytes)throw std::runtime_error("conflicting replay");
 } else {
  if(delta.post_image.bytes.size()>max_pending_bytes_||queued>max_pending_bytes_-delta.post_image.bytes.size())throw std::runtime_error("reorder budget exhausted");
  pending_.emplace(delta.sequence,delta);
 }
 while(data_.sequence!=UINT64_MAX) {
  auto it=pending_.find(data_.sequence+1);if(it==pending_.end())break;
  data_=decode(it->second.post_image);pending_.erase(it);
 }
 return data_.sequence;
}
std::vector<Delta> StatePartition::updates()const {std::lock_guard<std::mutex> l(mutex_);std::vector<Delta> out;for(auto& [n,d]:log_)out.push_back(d);return out;}
void StatePartition::acknowledge(uint64_t seq){std::lock_guard<std::mutex> l(mutex_);if(seq>data_.sequence)throw std::invalid_argument("ACK beyond produced sequence");log_.erase(log_.begin(),log_.upper_bound(seq));}
uint64_t StatePartition::dirty_bytes()const{std::lock_guard<std::mutex> l(mutex_);uint64_t bytes=0;for(auto& [n,d]:log_)bytes+=d.post_image.bytes.size();return bytes;}
Progress StatePartition::freeze(){std::lock_guard<std::mutex> l(mutex_);frozen_=true;return {data_.sequence,data_.input};}
Progress StatePartition::progress()const{std::lock_guard<std::mutex> l(mutex_);return {data_.sequence,data_.input};}
void StatePartition::stop_logging(){std::lock_guard<std::mutex> l(mutex_);logging_=false;ticket_.clear();log_.clear();}
std::map<int64_t,Aggregate> StatePartition::groups()const{std::lock_guard<std::mutex> l(mutex_);return data_.groups;}
std::vector<std::pair<int64_t,int64_t>> StatePartition::matches()const{std::lock_guard<std::mutex> l(mutex_);return data_.matches;}
std::vector<Row> StatePartition::sorted_rows()const{std::lock_guard<std::mutex> l(mutex_);return data_.rows;}
std::optional<std::pair<uint64_t,Row>> StatePartition::next_sorted(uint64_t gen) {
 std::lock_guard<std::mutex> l(mutex_);
 if(frozen_||gen!=generation_||data_.kind!=Kind::Sort||!data_.finalized)return std::nullopt;
 if(data_.output_cursor==data_.rows.size())return std::nullopt;
 auto next=data_;auto id=next.output_cursor++;auto row=next.rows[id];
 if(next.sequence==UINT64_MAX)throw std::overflow_error("log sequence overflow");++next.sequence;
 auto img=encode(next);
 if(logging_) {
  uint64_t outstanding=0;for(const auto& [seq,d]:log_)outstanding+=d.post_image.bytes.size();
  if(img.bytes.size()>max_pending_bytes_||outstanding>max_pending_bytes_-img.bytes.size())throw std::runtime_error("delta log budget exhausted; catch up before retry");
  log_.emplace(next.sequence,Delta{ticket_,next.sequence,img});
 }
 data_=std::move(next);return std::make_pair(id,row);
}
}
