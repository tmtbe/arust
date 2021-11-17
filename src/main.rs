#[macro_use]
extern crate rbatis;

use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::Mutex;
use chashmap::CHashMap;
use fast_log::fast_log::exit;
use futures::future::BoxFuture;
use futures::FutureExt;
use lazy_static::lazy_static;
use rbatis::crud::{CRUD, CRUDMut};
use rbatis::db::DBPoolOptions;
use rbatis::rbatis::Rbatis;
use itertools::Itertools;

lazy_static! {
    static ref RB:Rbatis=Rbatis::new();
    static ref CACHE:CHashMap<i64, TbUser>=Default::default();
    static ref COUNT:AtomicI64=AtomicI64::new(0);
    static ref LOCK:Mutex<i64>=Mutex::new(0);
}

#[tokio::main]
///mysql://test22:test2021!%40%23@rm-j6cpqm7zm633224ey8o.mysql.rds.aliyuncs.com:3306/cs1
async fn main() {
    let args: Vec<String> = env::args().collect();
    println!("{:?}", args);
    let url = args.get(1);
    if url.is_none() {
        println!("请输入mysql地址格式如下,有特殊字符请转义：『mysql://用户名:密码@host:3306/数据库』");
        return;
    }
    init_db(url.unwrap()).await;
    let mut handles = Vec::with_capacity(20);
    for i in 0..40 {
        handles.push(tokio::spawn(run_loop()));
    }
    for handle in handles {
        handle.await;
    }
    println!("{}", "全部任务结束");
}

async fn run_loop() {
    loop {
        let max = run_with_start_id().await;
        if max == -1 {
            break;
        }
    }
    println!("{}", "子任务结束");
}

#[crud_table]
#[derive(Clone, Debug)]
pub struct TbUser {
    pub id: Option<i64>,
    pub agentid: Option<i64>,
    pub username: Option<String>,
    pub treepath: Option<String>,
    pub createtime: Option<i64>,
}

async fn init_db(url: &String) {
    let mut opt = DBPoolOptions::new();
    opt.max_connections = 100;
    RB.link_opt(url.as_str(), opt).await.unwrap();
}

async fn find_by_id(id: i64) -> Option<TbUser> {
    return match CACHE.get(&id) {
        Some(user) => {
            Some(TbUser {
                id: user.id,
                agentid: user.agentid,
                username: user.username.clone(),
                treepath: user.treepath.clone(),
                createtime: user.createtime.clone(),
            })
        }
        _ => {
            let option: Option<TbUser> = RB.fetch_by_column("id", &id).await.unwrap();
            if option.is_some() {
                CACHE.insert(id, option.clone().unwrap());
            }
            option
        }
    };
}

async fn count_null_tree_path() -> i64 {
    let wrapper = RB.new_wrapper().is_null("treepath");
    RB.fetch_count_by_wrapper::<TbUser>(wrapper).await.unwrap() as i64
}

async fn find_null_tree_path(id: i64) -> Vec<TbUser> {
    let wrapper = RB.new_wrapper().is_null("treepath").and().gt("id", id).limit(80);
    RB.fetch_list_by_wrapper(wrapper).await.unwrap()
}

async fn run_with_start_id() -> i64 {
    let mut ids: Vec<i64> = Vec::new();
    let mut save_ids: Vec<i64> = Vec::new();
    let mut saves: HashMap<i64, TbUser> = HashMap::new();
    let mut max_id = -1;
    let path_null_user_list: Vec<TbUser>;
    {
        let mut start_id = LOCK.lock().await;
        if *start_id == -1 {
            return -1;
        }
        println!("Task 任务区间开始于:{}", start_id);
        path_null_user_list = find_null_tree_path(*start_id).await;
        for user in &path_null_user_list {
            let id = user.id.unwrap();
            if id > max_id {
                max_id = id;
            }
            ids.push(id);
            CACHE.insert(user.id.unwrap(), user.clone());
        }
        *start_id = max_id;
    }
    for user in path_null_user_list {
        let id = user.id.unwrap();
        get_tree_path(id, &mut saves).await;
    }
    for (&id, _user) in &saves {
        save_ids.push(id);
    }
    let mut tx = RB.acquire_begin().await.unwrap();
    if tx.remove_batch_by_column::<TbUser, i64>("id", save_ids.as_slice()).await.is_ok() {
        if tx.save_batch(saves.values().collect_vec().as_slice(), &[]).await.is_ok() {
            tx.commit().await.unwrap();
            COUNT.fetch_add(save_ids.len() as i64, Ordering::SeqCst);
        } else {
            tx.rollback().await.unwrap();
        }
    } else {
        tx.rollback().await.unwrap();
    }
    let count = count_null_tree_path().await;
    println!("剩余:{}", count);
    max_id
}

fn get_tree_path(id: i64, saves: &mut HashMap<i64, TbUser>) -> BoxFuture<String> {
    async move {
        let mut result: String = String::from("0");
        if id == 0 {
            return result;
        }
        let option_user = find_by_id(id).await;
        match option_user {
            Some(mut user) => {
                if let Some(treepath) = user.treepath {
                    result = treepath;
                    return result;
                } else if user.agentid.unwrap() == 0 {
                    result = String::from("0");
                } else {
                    result = get_tree_path(user.agentid.unwrap(), saves).await + "-" + user.agentid.unwrap().to_string().as_str();
                }
                user.treepath = Some(result.clone());
                saves.insert(user.id.unwrap(), user);
            }
            _ => result = String::from("NULL")
        }
        result
    }.boxed()
}