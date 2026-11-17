module.exports={apps:[{
  name:'api',script:require.resolve('./pm2-app.cjs'),exec_mode:'cluster',instances:2,
  wait_ready:true,listen_timeout:10000,kill_timeout:10000,
  env:{METRICS_SOCKET:process.env.METRICS_SOCKET,PORT:process.env.PORT ?? '3000'},
}]};
