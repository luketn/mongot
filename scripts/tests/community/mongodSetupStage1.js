rs.initiate();
sleep(5000);
db.getSiblingDB("admin").createUser({
    user: "admin",
    pwd: "password",
    roles: [{role: "root", db: "admin"}]
})
