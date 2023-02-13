package org.segment.kvctl.cli.hot

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.codehaus.groovy.control.CompilerConfiguration

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

@CompileStatic
@Slf4j
class RefreshLoader {
    private RefreshLoader() {}

    static RefreshLoader create(GroovyClassLoader gcl) {
        def r = new RefreshLoader()
        r.gcl = gcl
        r
    }

    private ScheduledExecutorService sh

    private List<String> dirList = []

    private List<String> classpathList = []

    private Map<String, Object> variables = [:]

    private GroovyClassLoader gcl

    private boolean isJarLoad = false

    RefreshLoader jarLoad(boolean flag) {
        this.isJarLoad = flag
        this
    }

    RefreshLoader addVariable(String key, Object value) {
        variables[key] = value
        this
    }

    RefreshLoader addDir(String dir) {
        dirList << dir
        this
    }

    RefreshLoader addClasspath(String classpath) {
        classpathList << classpath
        this
    }

    private GroovyShell getShell() {
        def config = new CompilerConfiguration()
        config.sourceEncoding = CachedGroovyClassLoader.GROOVY_FILE_ENCODING
        config.classpath = classpathList
        if (classpathList) {
            for (classpath in classpathList) {
                gcl.addClasspath(classpath)
            }
        }

        def b = new Binding()
        variables.each { k, v ->
            b.setProperty(k, v)
        }
        new GroovyShell(gcl, b, config)
    }

    private void loadAndRun(String packageNameDir) {
        def dirs = this.getClass().classLoader.getResources(packageNameDir)
        for (url in dirs) {
            if ('jar' == url.protocol) {
                def jarFile = ((JarURLConnection) url.openConnection()).jarFile
                for (entry in jarFile.entries()) {
                    if (entry.isDirectory()) {
                        continue
                    } else {
                        String name = entry.name
                        runGroovyScriptInJar(name, packageNameDir)
                    }
                }
            }
        }
    }

    private void runGroovyScriptInJar(String name, String packageNameDir) {
        if (name[0] == '/') {
            name = name[1..-1]
        }

        if (name.startsWith(packageNameDir) && name.endsWith('.class') && !name.contains('$') && !name.contains('_')) {
            String className = name[0..-7].replaceAll(/\//, '.')
            def one = Class.forName(className).newInstance()
            if (one instanceof Script) {
                Script gs = one as Script
                def b = new Binding()
                variables.each { k, v ->
                    b.setProperty(k, v)
                }
                gs.setBinding(b)
                gs.run()
                log.info('run script {}', name)
            }
        }
    }

    void refresh() {
        def shell = getShell()
        for (dir in dirList) {
            if (isJarLoad) {
                def index = dir.indexOf('/src/')
                def packageNameDir = dir[index + 5..-1]
                loadAndRun(packageNameDir)
                continue
            }

            def d = new File(dir)
            if (!d.exists() || !d.isDirectory()) {
                continue
            }

            d.eachFileRecurse { File f ->
                if (f.isDirectory()) {
                    return
                }

                if (!f.name.endsWith(CachedGroovyClassLoader.GROOVY_FILE_EXT)) {
                    return
                }

                refreshFile(f, shell)
            }
        }
    }

    private Map<String, Long> lastModified = [:]

    void refreshFile(File file, GroovyShell shell = null) {
        if (shell == null) {
            shell = getShell()
        }

        def l = lastModified[file.absolutePath]
        if (l != null && l.longValue() == file.lastModified()) {
            return
        }

        def name = file.name
        try {
            shell.evaluate(file)
            lastModified[file.absolutePath] = file.lastModified()
            log.info 'done refresh {}', name
            if (refreshFileCallback != null) {
                refreshFileCallback.call(file)
            }
        } catch (Exception e) {
            log.error('fail eval - ' + name, e)
        }
    }

    private Closure<Void> refreshFileCallback

    RefreshLoader refreshFileCallback(Closure<Void> refreshFileCallback) {
        this.refreshFileCallback = refreshFileCallback
        this
    }

    void stop() {
        if (sh) {
            sh.shutdown()
            log.info 'stop runner refresh loader interval'
            sh = null
        }
    }

    void start() {
        if (isJarLoad) {
            refresh()
            return
        }

        sh = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory('runner-refresh'))
        sh.scheduleWithFixedDelay({
            try {
                refresh()
            } catch (Exception e) {
                log.error('fail runner refresh', e)
            }
        }, 10, 10, java.util.concurrent.TimeUnit.SECONDS)
        log.info 'start runner refresh loader interval'
    }
}
