const { getResult } = require('./1.js');
const fs = require('fs');
const path = require('path');

(async () => {
  const result = await getResult();

  if (!result) {
    console.log('结果尚未准备好，请稍后重试');
    return;
  }

  const filePath = path.join(__dirname, 'result.json');

  try {
    fs.writeFileSync(filePath, JSON.stringify(result, null, 2), 'utf-8');
    console.log('✅ 结果已成功写入 result.json');
  } catch (err) {
    console.error('❌ 写入 JSON 文件出错:', err);
  }
})();
