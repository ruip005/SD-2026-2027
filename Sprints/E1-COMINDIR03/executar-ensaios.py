"""Ensaios reais com Kubo. Processos separados, provas persistentes e falhas em cópias isoladas."""
from pathlib import Path
import argparse, base64, datetime as dt, difflib, hashlib, json, os, socket, subprocess, time, urllib.request
ROOT=Path(__file__).resolve().parent
HIDDEN=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0
HTTP=urllib.request.build_opener(urllib.request.ProxyHandler({}))
API='http://127.0.0.1:5001/api/v0'
def rpc(cmd): return json.loads(HTTP.open(urllib.request.Request(API+'/'+cmd,method='POST'),timeout=5).read())
def run(cmd,cwd=None,timeout=120):
    return subprocess.run(list(map(str,cmd)),cwd=cwd,capture_output=True,encoding='utf-8',errors='replace',timeout=timeout,creationflags=HIDDEN)
def wait(pred,why,timeout=30):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        if pred(): return
        time.sleep(.15)
    raise RuntimeError('Tempo esgotado: '+why)
def read(p): return p.read_text(encoding='utf-8') if p.exists() else ''
def topics():
    result=set()
    for t in rpc('pubsub/ls').get('Strings',[]) or []:
        if t.startswith('u'):
            try: t=base64.urlsafe_b64decode(t[1:]+'='*(-len(t[1:])%4)).decode()
            except (ValueError,UnicodeDecodeError): pass
        result.add(t)
    return result
def rows(folder,pattern='lider-p*.txt'):
    result=[]
    for p in sorted(folder.glob(pattern)):
        k=int(p.stem.rsplit('p',1)[1])
        for line in read(p).splitlines():
            f=line.split(';')
            if len(f)!=6: continue
            result.append(dict(k=k,seq=int(f[0]),sensor=f[1],temp=float(f[2]),time=f[3],L=int(f[4]),V=list(map(int,f[5].split(','))),line=line))
    return result
def compile_projects():
    for role in ['produtor','lider','replica','processador']:
        p=ROOT/('rep03-'+role)
        r=run(['javac','--release','11','-encoding','UTF-8','-d',p/'out',*sorted((p/'src/rep03').glob('*.java'))])
        if r.returncode: raise RuntimeError(r.stdout+r.stderr)
class System:
    def __init__(self,folder,variants=None,processor=True,loss=False):
        self.folder=folder; self.variants=variants or {}; self.processor=processor; self.loss=loss
        self.processes={}; self.logs=[]
    def start(self,name,role,main,*args,props=()):
        self.folder.mkdir(parents=True,exist_ok=True)
        index=len(list(self.folder.glob(name+'*.stdout.log')))
        stem=name if index==0 else name+'-restart'+str(index)
        out=open(self.folder/(stem+'.stdout.log'),'wb'); err=open(self.folder/(stem+'.stderr.log'),'wb')
        self.logs.extend([out,err])
        cp=self.variants.get(role,ROOT/('rep03-'+role)/'out')
        cmd=['java','-Dfile.encoding=UTF-8','-Dstdout.encoding=UTF-8','-Dstderr.encoding=UTF-8',*props,'-cp',str(cp),'rep03.'+main,*map(str,args)]
        p=subprocess.Popen(cmd,cwd=self.folder,stdin=subprocess.PIPE,stdout=out,stderr=err,creationflags=HIDDEN)
        self.processes[name]=p
        with (self.folder/'processos.jsonl').open('a',encoding='utf-8') as f:
            f.write(json.dumps(dict(at=dt.datetime.now(dt.timezone.utc).isoformat(),pid=p.pid,name=name,command=cmd),ensure_ascii=False)+'\n')
        return p
    def stop(self,name):
        p=self.processes.pop(name,None)
        if p and p.poll() is None:
            p.terminate()
            try: p.wait(timeout=8)
            except subprocess.TimeoutExpired: p.kill(); p.wait(timeout=5)
    def __enter__(self):
        try:
            self.folder.mkdir(parents=True,exist_ok=True)
            if (self.folder/'processos.jsonl').exists():
                raise RuntimeError('A pasta já contém um ensaio; escolher uma pasta nova: '+str(self.folder))
            for k in range(3): (self.folder/f'lider-p{k}.txt').touch()
            # Representa também as partições vazias das réplicas, para o PartitionCheck.
            for ident,ks in [(1,[0,1]),(2,[1,2]),(3,[2,0])]:
                for k in ks: (self.folder/f'replica-{ident}-p{k}.txt').touch()
            for ident,ks in [(1,'0,1'),(2,'1,2'),(3,'2,0')]:
                props=['-Drep03.traceWrites=escritas-replica-'+str(ident)+'.csv']
                if self.loss and ident==1: props.append('-Ddescartar=1')
                self.start('replica'+str(ident),'replica','PartitionReplica',ident,ks,props=props)
            wait(lambda:{'rep03-p0','rep03-p1','rep03-p2'}<=topics(),'réplicas subscritas'); time.sleep(.8)
            for k in range(3): self.start('lider'+str(k),'lider','PartitionLeader',k,3)
            wait(lambda:{'rep03-in-p0','rep03-in-p1','rep03-in-p2'}<=topics(),'líderes subscritos')
            if self.processor:
                self.start('processador','processador','AlarmProcessor',3); time.sleep(1.5)
            for name,p in self.processes.items():
                if p.poll() is not None: raise RuntimeError('Terminou no arranque: '+name)
            self.producer(); return self
        except BaseException:
            self.__exit__(); raise
    def producer(self,n=3):
        self.stop('produtor'); self.start('produtor','produtor','Producer',n); time.sleep(.4)
    def send(self,command):
        with (self.folder/'comandos.txt').open('a',encoding='utf-8') as f: f.write(command+'\n')
        p=self.processes['produtor']; p.stdin.write((command+'\n').encode()); p.stdin.flush()
    def settle(self):
        prev=None; stable=time.monotonic(); end=time.monotonic()+60
        while time.monotonic()<end:
            state=[(p.name,p.stat().st_size) for p in sorted(self.folder.glob('*.txt'))]
            if state!=prev: prev=state; stable=time.monotonic()
            if time.monotonic()-stable>3: return
            time.sleep(.2)
        raise RuntimeError('Ficheiros não estabilizaram')
    def __exit__(self,*exc):
        for name in list(self.processes): self.stop(name)
        for f in self.logs: f.close()
        wait(lambda:not any(t.startswith('rep03-') for t in topics()),'fechar subscrições',15); time.sleep(.3)
def check(folder):
    p=run(['java','-Dfile.encoding=UTF-8','-Dstdout.encoding=UTF-8','-cp',ROOT/'rep03-replica/out','rep03.CausalCheck',3,folder])
    (folder/'causalcheck.txt').write_text(p.stdout+p.stderr,encoding='utf-8')
    rr=rows(folder); causal=concurrent=distinct=0
    for i,a in enumerate(rr):
        for b in rr[i+1:]:
            if a['k']==b['k']: continue
            ab=all(x<=y for x,y in zip(a['V'],b['V'])) and a['V']!=b['V']
            ba=all(y<=x for x,y in zip(a['V'],b['V'])) and a['V']!=b['V']
            if ab or ba: causal+=1
            elif a['V']!=b['V']: concurrent+=1; distinct+=a['L']!=b['L']
    before=[]; after=[]; vb=[]
    for r in rr:
        prefix='REG;127.0.0.1;'+str(7000+r['k'])+';'
        before.append(len((prefix+';'.join(r['line'].split(';')[:4])).encode()))
        after.append(len((prefix+r['line']).encode()))
        vb.append(len(','.join(map(str,r['V'])).encode()))
    result=dict(exit=p.returncode,output=p.stdout+p.stderr,registos=[sum(r['k']==k for r in rr) for k in range(3)],
        alarmes_gravados=sum(r['sensor'].startswith('ALARM-') for r in rr),originais=sum(not r['sensor'].startswith('ALARM-') for r in rr),
        alarmes_publicados=read(folder/'processador.stdout.log').count('ALARME:'),causais=causal,concorrentes=concurrent,
        concorrentes_L_distinto=distinct,proporcao=distinct/concurrent if concurrent else 0,
        REG_sem_carimbos_media_bytes=sum(before)/len(before) if before else 0,
        REG_com_carimbos_media_bytes=sum(after)/len(after) if after else 0,vetor_media_bytes=sum(vb)/len(vb) if vb else 0)
    p=run(['java','-Dfile.encoding=UTF-8','-Dstdout.encoding=UTF-8','-cp',ROOT/'rep03-replica/out','rep03.PartitionCheck',3,folder])
    (folder/'partitioncheck.txt').write_text(p.stdout+p.stderr,encoding='utf-8'); result['partitioncheck_exit']=p.returncode
    (folder/'metricas.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(f'{folder.name}: {result["registos"]}, causais={causal}, concorrentes={concurrent}, check={result["exit"]}',flush=True)
    return result
def normal(base):
    folder=base/'7a-figura'; print('Arrancar 7a: Figura 1',flush=True)
    with System(folder) as s:
        s.send('S02 29.3'); wait(lambda:len(rows(folder))==2,'leitura e alarme')
        s.send('S01 21.0'); wait(lambda:len(rows(folder))==3,'evento b')
        s.send('S04 22.5'); wait(lambda:len(rows(folder))==4,'evento d'); s.settle()
    actual={(r['k'],r['seq']):(r['L'],r['V']) for r in rows(folder)}
    if actual!={(1,1):(1,[0,1,0]),(2,1):(2,[0,1,1]),(0,1):(1,[1,0,0]),(0,2):(2,[2,0,0])}: raise RuntimeError('Figura diferente: '+repr(actual))
    check(folder)
    for label,processor in [('7b-sem-processador',False),('7c-com-processador-1',True),('7c-com-processador-2',True),('7c-com-processador-3',True)]:
        print('Arrancar '+label,flush=True); folder=base/label
        with System(folder,processor=processor) as s:
            s.send('rajada 300 10'); s.send('sair'); s.processes['produtor'].wait(timeout=120); s.settle()
        r=check(folder)
        if r['exit'] or r['originais']!=300: raise RuntimeError('Rajada incompleta/inválida: '+label)
    restart(base)
    print('Arrancar 7e: perda e ordem de escrita',flush=True); folder=base/'7e-entrega-sem-ordem-causal'
    with System(folder,loss=True) as s:
        s.send('S01 29.3'); wait(lambda:(folder/'replica-1-p1.txt').exists() and (folder/'replica-1-p1.txt').stat().st_size>0,'alarme na réplica 1')
        for i in range(12): s.send('S01 '+str(20+(i+1)/10))
        s.send('sair'); s.processes['produtor'].wait(timeout=60); s.settle()
        wait(lambda:len(rows(folder,'replica-1-p0.txt'))==13,'origem recuperada')
    traces=[]
    for line in read(folder/'escritas-replica-1.csv').splitlines():
        f=line.split(';'); traces.append(dict(wall=f[0],start=int(f[1]),end=int(f[2]),file=f[3],seq=int(f[4]),sensor=f[5]))
    alarm=next(t for t in traces if t['sensor']=='ALARM-S01'); origin=next(t for t in traces if t['sensor']=='S01' and t['seq']==1)
    proof=dict(alarme=alarm,origem=origin,alarme_antes_da_origem=alarm['end']<origin['start'],intervalo_minimo_ms=(origin['start']-alarm['end'])/1e6,perda_simulada='-Ddescartar=1 apenas na réplica 1')
    if not proof['alarme_antes_da_origem']: raise RuntimeError('Inversão não demonstrada')
    (folder/'ordem-escrita.json').write_text(json.dumps(proof,ensure_ascii=False,indent=2),encoding='utf-8'); check(folder)
def restart(base):
    print('Arrancar 7d: reinício p2',flush=True); folder=base/'7d-reinicio'
    with System(folder) as s:
        s.send('S02 29.3'); wait(lambda:len(rows(folder))==2,'alarme antes do reinício')
        before=next(r for r in rows(folder) if r['k']==2)
        wait(lambda:len(rows(folder,'replica-2-p2.txt'))==1 and len(rows(folder,'replica-3-p2.txt'))==1,'entrega antes do reinício')
        s.stop('lider2'); wait(lambda:'rep03-in-p2' not in topics(),'fechar p2')
        s.start('lider2','lider','PartitionLeader',2,3); wait(lambda:'rep03-in-p2' in topics(),'reabrir p2'); time.sleep(.5)
        s.send('S03 22.0'); wait(lambda:sum(r['k']==2 for r in rows(folder))==2,'evento após reinício')
        after=[r for r in rows(folder) if r['k']==2][-1]
        if (after['seq'],after['L'],after['V'])!=(2,3,[0,1,2]): raise RuntimeError('Reinício incorreto')
        (folder/'reinicio.json').write_text(json.dumps(dict(antes=before,depois=after),ensure_ascii=False,indent=2),encoding='utf-8'); s.settle()
    check(folder)
def variant(base,name,role,transform):
    dst=ROOT/'ensaios-locais/variantes'/base.name/name; src=dst/'src/rep03'; src.mkdir(parents=True,exist_ok=True); patches=[]
    for f in sorted((ROOT/('rep03-'+role)/'src/rep03').glob('*.java')):
        old=read(f); new=transform(f.name,old); (src/f.name).write_text(new,encoding='utf-8')
        if old!=new: patches.extend(difflib.unified_diff(old.splitlines(True),new.splitlines(True),fromfile='correto/'+f.name,tofile=name+'/'+f.name))
    if not patches: raise RuntimeError('Variante sem alteração: '+name)
    evidence=base/'variantes'; evidence.mkdir(exist_ok=True); (evidence/(name+'.patch')).write_text(''.join(patches),encoding='utf-8')
    cp=dst/'out'; p=run(['javac','--release','11','-encoding','UTF-8','-d',cp,*sorted(src.glob('*.java'))])
    if p.returncode: raise RuntimeError(p.stdout+p.stderr)
    return {role:cp}
def faults(base):
    for name,old,new in [('8a-sem-merge','                nextV.merge(receivedV);','                // FALHA: merge omitido'),('8b-lamport-sem-mensagem','Math.incrementExact(Math.max(lamport, receivedL))','Math.incrementExact(lamport)')]:
        print('Arrancar '+name,flush=True)
        v=variant(base,name,'lider',lambda f,s:s.replace(old,new) if f=='PartitionLeader.java' else s); folder=base/name
        with System(folder,variants=v) as s:
            s.send('S02 29.3'); wait(lambda:len(rows(folder))==2,'leitura e alarme com falha'); s.settle()
        check(folder)
    name='8c-reinicio-zero'; print('Arrancar '+name,flush=True)
    old='            throw new IllegalArgumentException("Estado persistido incompatível com a partição/N");'
    new=old+'\n        // FALHA: conserva seq, mas reinicia L e V.\n        this.lamport = 0;\n        this.vector = new VectorClock(partitioner.count());'
    v=variant(base,name,'lider',lambda f,s:s.replace(old,new) if f=='PartitionLeader.java' else s); folder=base/name
    with System(folder) as s:
        s.send('S02 29.3'); wait(lambda:len(rows(folder))==2,'alarme inicial')
        s.stop('lider2'); wait(lambda:'rep03-in-p2' not in topics(),'fechar p2'); s.variants=v
        s.start('lider2','lider','PartitionLeader',2,3); wait(lambda:'rep03-in-p2' in topics(),'reabrir p2'); time.sleep(.5)
        s.send('S03 22.0'); wait(lambda:'REJEITADO: V[k]' in read(folder/'lider2-restart1.stdout.log'),'guarda V[k]==seq'); s.settle()
    check(folder)
    def no_copy(f,s):
        if f!='AlarmProcessor.java': return s
        s=s.replace('        String alarm = null;','        String alarm = null;\n        VectorClock pendingVector = null;\n        long pendingLamport = 0;\n        String pendingSensor = null;')
        s=s.replace('                alarm = Wire.in(sensor, r.getTemperature(), lamport, vector.copy());','                pendingVector = vector; // FALHA: referência sem copy()\n                pendingLamport = lamport;\n                pendingSensor = sensor;')
        s=s.replace('        if (alarm != null) {', '''        if (pendingVector != null) {
            System.out.println("SNAPSHOT_CAPTURADO L=" + pendingLamport + " V=" + pendingVector);
            // Barreira experimental para reproduzir uma intercalação entre threads.
            long deadline = System.nanoTime() + 15000000000L;
            while (!java.nio.file.Files.exists(java.nio.file.Path.of("libertar-snapshot"))) {
                if (System.nanoTime() > deadline) throw new IllegalStateException("Barreira expirou");
                try { Thread.sleep(10); } catch (InterruptedException e) { return false; }
            }
            alarm = Wire.in(pendingSensor, r.getTemperature(), pendingLamport, pendingVector);
            System.out.println("SNAPSHOT_PUBLICADO " + alarm);
        }
        if (alarm != null) {''')
        return s.replace('            vector.merge(r.getVector());','            vector.merge(r.getVector());\n            System.out.println("OBSERVADO p" + k + "#" + r.getSeq() + " L=" + lamport + " V=" + vector);')
    for name,copy in [('8d-snapshot-sem-copy',False),('8d-controlo-com-copy',True)]:
        print('Arrancar '+name,flush=True)
        def transform(f,s):
            result=no_copy(f,s)
            return result.replace('pendingVector = vector; // FALHA: referência sem copy()','pendingVector = vector.copy(); // CONTROLO: snapshot independente') if copy else result
        v=variant(base,name,'processador',transform); folder=base/name
        with System(folder,variants=v) as s:
            s.send('S02 29.3'); wait(lambda:'SNAPSHOT_CAPTURADO' in read(folder/'processador.stdout.log'),'captura snapshot')
            s.send('S01 21.0'); s.send('S04 22.5'); wait(lambda:'OBSERVADO p0#2' in read(folder/'processador.stdout.log'),'mutação por outra thread')
            (folder/'libertar-snapshot').touch(); wait(lambda:len(rows(folder))==4,'alarme com snapshot'); s.settle()
        check(folder)
    print('Arrancar 8e: produtor N=4',flush=True); folder=base/'8e-N-incompativel'
    with System(folder) as s:
        s.producer(4); s.send('S02 29.3'); s.send('S04 22.5'); s.send('sair'); s.processes['produtor'].wait(timeout=30)
        wait(lambda:'REJEITADO: IN;S02' in read(folder/'lider1.stdout.log'),'vetor N=4 rejeitado'); s.settle()
    check(folder)
def main():
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument('--fase',choices=['normal','falhas','reinicio','tudo'],default='tudo'); parser.add_argument('--pasta',type=Path); a=parser.parse_args()
    base=a.pasta.resolve() if a.pasta else ROOT/'ensaios'/dt.datetime.now().strftime('%Y%m%d-%H%M%S'); base.mkdir(parents=True,exist_ok=True)
    print('Provas em '+str(base),flush=True); version=rpc('version')
    if any(t.startswith('rep03-') for t in topics()): raise RuntimeError('Fechar programas rep03 ativos antes dos ensaios')
    for port in [7000,7001,7002]:
        with socket.socket() as sock:
            try: sock.bind(('127.0.0.1',port))
            except OSError: raise RuntimeError('Porto ocupado: '+str(port))
    compile_projects()
    (base/('ambiente-'+a.fase+'.json')).write_text(json.dumps(dict(inicio_utc=dt.datetime.now(dt.timezone.utc).isoformat(),kubo=version,java=run(['java','-version']).stderr,N=3,
        modelo='um daemon Kubo; processos Java separados; mesma máquina',hashes={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in ROOT.glob('rep03-*/src/rep03/*.java')}),ensure_ascii=False,indent=2),encoding='utf-8')
    if a.fase in ['normal','tudo']: normal(base)
    if a.fase=='reinicio': restart(base)
    if a.fase in ['falhas','tudo']: faults(base)
    summary={p.parent.name:json.loads(read(p)) for p in sorted(base.glob('*/metricas.json'))}
    (base/'resumo.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print('ENSAIOS CONCLUÍDOS: '+str(base),flush=True)
if __name__=='__main__': main()
