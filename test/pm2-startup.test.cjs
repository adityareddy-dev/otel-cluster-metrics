const {test} = require('node:test');
const assert = require('node:assert/strict');
const {mkdtemp,rm} = require('node:fs/promises');
const {tmpdir} = require('node:os');
const path = require('node:path');
const {execFile} = require('node:child_process');
const {promisify} = require('node:util');
const run = promisify(execFile);

for(const [mode,name,expected] of [
  ['connect','a timed out PM2 startup closes its late connection',/Startup rejected[\s\S]*PM2 connect completed/],
  ['connect-error','a timed out PM2 startup closes its late failed connection',/Startup rejected[\s\S]*PM2 connect completed/],
  ['listen','a timed out collector startup closes its late listening socket',/Startup rejected[\s\S]*Socket listen completed/],
  ['list-before','shutdown closes a late PM2 reconnect from the first membership list',/Collection rejected[\s\S]*PM2 list completed/],
  ['list-after','shutdown closes a late PM2 reconnect from the second membership list',/Collection rejected[\s\S]*PM2 list completed/],
  ['list-recover','a slow PM2 list preserves a healthy client for the next collection',/Collection rejected[\s\S]*PM2 list completed[\s\S]*Collection recovered/],
]) test(`${name} and the child exits naturally`,{timeout:25000},async t=>{
  const home=await mkdtemp(path.join(process.env.OCM_PM2_TEST_HOME || tmpdir(),'startup-'));
  const env={...process.env,PM2_HOME:home};
  t.after(async()=>{
    try {
      // Kill only this test's daemon, including when the child leaks and times out.
      await run(process.execPath,[require.resolve('pm2/bin/pm2'),'kill'],{env,timeout:15000});
    } finally {
      await rm(home,{recursive:true,force:true});
    }
  });
  let output;
  try {
    ({stdout:output}=await run(process.execPath,[path.join(__dirname,'pm2-timeout-child.cjs'),mode],{env,timeout:5000}));
  } catch(error) {
    assert.fail(`Collector did not exit cleanly within 5 seconds: ${error.message}\n${error.stdout}\n${error.stderr}`);
  }
  assert.match(output,expected);
  assert.match(output,/No open collector handles/);
});
