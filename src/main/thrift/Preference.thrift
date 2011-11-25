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

struct Results {
  # byte-packed list of i64, little-endian:
  1: binary ids
  2: i64 next_cursor
  3: i64 prev_cursor
}

struct PrefResults {
  1: list<Preference> prefs
  2: i64 next_cursor
  3: i64 prev_cursor
}

exception PrefException {
  1: string description
}

service PreferenceService {
  void create(1:i32 graph_id,2: i64 user_id, 3: i64 item_id,4:string source,5:string action,6:double score,7:i32 create_date,8:i32 create_type,9:i32 status) throws(1: PrefException ex)
  void createPreference(1:i32 graph_id,2: Preference pref) throws(1: PrefException ex)
 
  void remove(1:i32 graph_id,2: i64 user_id, 3:i64 item_id, 4: string source, 5: string action) throws(1: PrefException ex)
  void removePreference(1:i32 graph_id,2: Preference pref) throws(1: PrefException ex)

  void update(1:i32 graph_id,2: i64 user_id, 3: i64 item_id,4:string source,5:string action,6:double score,7:i32 create_date,8:i32 create_type,9:i32 status) throws(1: PrefException ex)
  void updatePreference(1:i32 graph_id,2:Preference pref)throws(1: PrefException ex)
  
  Preference selectByUserItemSourceAndAction(1:i32 graph_id,2: i64 user_id, 3: i64 item_id, 4: string source, 5: string action) throws(1: PrefException ex)
  
  list<Preference> selectByUserSourceAndAction(1:i32 graph_id,2: i64 user_id, 3: string source, 4: string action) throws(1: PrefException ex)
  PrefResults selectPageByUserSourceAndAction(1:i32 graph_id,2: i64 user_id, 3: string source, 4: string action, 5:i64 item_cursor, 6:i32 count) throws(1: PrefException ex)
  
  list<Preference> selectBySourcAndAction(1:i32 graph_id,2: string source, 3: string action)throws(1: PrefException ex)   
  PrefResults selectPageBySourcAndAction(1:i32 graph_id,2: string source, 3: string action, 4:i64 user_cursor,5:i64 item_cursor, 6:i32 count) throws(1: PrefException ex)
  

  list<Preference> selectByUser(1:i32 graph_id,2: i64 user_id) throws(1: PrefException ex)
  PrefResults selectPageByUser(1:i32 graph_id,2: i64 user_id,  3:i64 item_cursor, 4:i32 count) throws(1: PrefException ex)
  
  list<Preference> selectAll(1:i32 graph_id,2: i64 user_id) throws(1: PrefException ex)
  PrefResults selectAllPage(1:i32 graph_id,2: i64 user_id, 3:i64 user_cursor,4:i64 item_cursor, 5:i32 count) throws(1: PrefException ex)
  
  binary selectUserIdsBySource(1:i32 graph_id,2:string source) throws(1: PrefException ex)


}
