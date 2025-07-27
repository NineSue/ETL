-- MySQL创建表的正确语法
CREATE TABLE IF NOT EXISTS connections (
                                           id INT PRIMARY KEY AUTO_INCREMENT,
                                           name VARCHAR(255) NOT NULL UNIQUE,
                                           type VARCHAR(50) NOT NULL,
                                           config TEXT NOT NULL,
                                           created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                           updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);