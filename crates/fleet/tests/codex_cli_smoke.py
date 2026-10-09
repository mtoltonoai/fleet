"""Run Fleet's real CLI against a fake Codex process; never calls a model."""
import json, os, pathlib, signal, socket, subprocess, sys, tempfile, time

binary = pathlib.Path(sys.argv[1] if len(sys.argv)>1 else 'target/debug/fleet').resolve()
mock = '''#!/usr/bin/env python3
import json,sys
for line in sys.stdin:
 r=json.loads(line); method=r.get('method'); i=r.get('id'); p=r.get('params',{})
 def send(x): print(json.dumps(x),flush=True)
 if method=='initialize': send({'id':i,'result':{}})
 elif method in ('thread/start','thread/resume'):
  assert p['model']=='gpt-6-astra',p
  send({'id':i,'result':{'thread':{'id':'smoke-thread'}}})
 elif method=='turn/start':
  send({'id':i,'result':{'turn':{'id':'smoke-turn'}}})
  if 'block' not in p['input'][0]['text']:
   send({'method':'turn/completed','params':{'threadId':'smoke-thread','turn':{'id':'smoke-turn','status':'completed','error':None}}})
 elif method=='turn/interrupt':
  send({'id':i,'result':{}})
  send({'method':'turn/completed','params':{'threadId':'smoke-thread','turn':{'id':'smoke-turn','status':'interrupted','error':None}}})
'''
with tempfile.TemporaryDirectory(prefix='fleet-cli-',dir='/tmp') as temporary:
 root=pathlib.Path(temporary)
 (root/'bin').mkdir(); (root/'bin/codex').write_text(mock); (root/'bin/codex').chmod(0o700)
 cfg=root/'config.toml'; cfg.write_text(f'root = "{root}/runtime"\nhub = "{root}/hub"\n')
 env=dict(os.environ,PATH=str(root/'bin')+':'+os.environ['PATH'],CDZ_KICKOFF='first bounded unit')
 env.pop('FLEET_BOARD_NATIVE',None)
 command=[str(binary),'--config',str(cfg),'codex-session','--agent','tester','--model','opus','--interval','24h']
 process=subprocess.Popen(command,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 statepath=root/'runtime/sessions/tester/state.json'
 def state():
  try: return json.loads(statepath.read_text())
  except (FileNotFoundError,json.JSONDecodeError): return {}
 def wait_status(status):
  deadline=time.monotonic()+10
  while time.monotonic()<deadline:
   s=state()
   if s.get('status')==status: return s
   if process.poll() is not None: raise AssertionError(f'host exited {process.returncode}: {process.stderr.read().decode()}')
   time.sleep(.03)
  raise AssertionError((status,state()))
 def control(body):
  with socket.socket(socket.AF_UNIX) as sock:
   sock.settimeout(3); sock.connect(str(statepath.parent/'control.sock'))
   sock.sendall(json.dumps(body).encode()+b'\n')
   return json.loads(sock.makefile('rb').readline())
 try:
  deadline=time.monotonic()+10
  while time.monotonic()<deadline:
   s=state()
   if s.get('status')=='idle' and s.get('last_outcome')=='completed': break
   time.sleep(.03)
  else: raise AssertionError(('initial outcome',state()))
  duplicate=subprocess.run(command,env=env,capture_output=True,timeout=5)
  assert duplicate.returncode != 0 and b'already owns' in duplicate.stderr
  assert control({'prompt':'block until interrupted'})['ok']
  wait_status('busy')
  assert control({'interrupt':True})['ok']
  wait_status('paused')
  assert not control({'prompt':'ordinary wake must not resume'})['ok']
  deadline=time.monotonic()+10
  while time.monotonic()<deadline and state().get('last_outcome')!='interrupted': time.sleep(.03)
  assert state()['last_outcome']=='interrupted'
  resumed=subprocess.run([str(binary),'--config',str(cfg),'resume-session','tester','--confirm-stopped'],env=env,capture_output=True,timeout=5)
  assert resumed.returncode==0,resumed.stderr
  wait_status('idle')
  assert state()['thread_id']=='smoke-thread'
  assert list(statepath.parent.glob('turn-*/result.json'))
  assert list(statepath.parent.glob('turn-*/stderr.log'))
  print('PASS: real CLI, Astra fallback, exclusive host, wake, cancellation, explicit resume, private artifacts; mocked Codex only')
 finally:
  process.send_signal(signal.SIGTERM)
  try: process.communicate(timeout=5)
  except subprocess.TimeoutExpired:
   process.kill(); process.communicate(); raise
