/*
 * The Table queries for the Py-ETL project.
 */

-- For CSV mock data
CREATE TABLE users
(
	id SERIAL,
	last_name VARCHAR(255),
	first_name VARCHAR(255),
	email VARCHAR(255),
	street VARCHAR(255),
	city VARCHAR(255),
	state VARCHAR(2),
	zip INT,
	pass_hash VARCHAR(255),
	PRIMARY KEY(id)
);

-- For mock JSON data
CREATE TABLE servers
(
	id SERIAL,
	host_name VARCHAR(255),
	ip_address VARCHAR(255),
	mac_address VARCHAR(255),
	city VARCHAR(255),
	country VARCHAR(255),
	PRIMARY KEY(id)
);

-- For mock Excel data
CREATE TABLE employees
(
	id SERIAL,
	last_name VARCHAR(255) NOT NULL,
	first_name VARCHAR(255) NOT NULL,
	job VARCHAR(255) NOT NULL,
	email VARCHAR(255) NOT NULL,
	password_hash VARCHAR(300) NOT NULL,
	phone VARCHAR(35),
	home_address VARCHAR(255) NOT NULL,
	city VARCHAR(150) NOT NULL,
	state VARCHAR(255) NOT NULL,
	zip INT NOT NULL
);

-- For API data
CREATE TABLE stocks
(
	id SERIAL,
	symbol VARCHAR(255),
	current_value DECIMAL,
	company VARCHAR(255),
	market VARCHAR(10),
	PRIMARY KEY(id)
);