db = db.getSiblingDB('cryptoDataDB');
db.createUser({
	user: "asif23",
	pwd: "sayan",
	roles: [
		{
			role: "readWrite",
			db: "cryptoDataDB"
		}
	],
});
