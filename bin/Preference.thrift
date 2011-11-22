namespace java com.ibm.webahead.steer.gizzard.preference.thrift
namespace java me.huiwen.prefz.thrift
namespace rb Preference

struct Preference {
  1: i64 user_id
  2: i64 item_id
  3: string source
  4: string action
  5: double score
  6: i32 create_date
  7: i32 status
  8: i32 create_type
}

struct Page {
  1: i32 count
  2: i64 cursor
}

struct PrefResults {
  1: list<Preference> prefs
  2: i64 next_cursor
  3: i64 prev_cursor
}

exception PrefzException {
  1: string description
}

service PreferenceService {
  void create(1: i64 userid, 2: i64 itemid,3:double score,4:string source,5:string action,6:i32 createdate,7:i32 createtype) throws(1: PrefzException ex)
  void destroy(1: Preference pref) throws(1: PrefzException ex)

  list<Preference> read(1: i64 userid) throws(1: PrefzException ex)
  
  list<Preference> selectPreferencesBySourcAndAction(1:string source,2:string action)throws(1: PrefzException ex)
  
}
