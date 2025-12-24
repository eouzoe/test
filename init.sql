-- 專案表：加入索引優化
CREATE TABLE IF NOT EXISTS projects (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    owner_id INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 建立複合索引：針對「作者+建立時間」的常見查詢優化
CREATE INDEX idx_owner_created ON projects(owner_id, created_at DESC);

-- 插入測試數據
INSERT INTO projects (title, description, owner_id) VALUES 
('超強 Go 專案', '這是一個碾壓 Ruby 的範例', 1),
('實習判官殺手', '解決所有效能與安全漏洞', 1);
