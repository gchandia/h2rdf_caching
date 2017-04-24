package gr.ntua.h2rdf.concurrent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

public class MyClassLoader extends ClassLoader{

    public MyClassLoader(ClassLoader parent) {
        super(parent);
    }

    public Class loadClass(String name) throws ClassNotFoundException {
        if(!name.startsWith("gr.ntua.h2rdf.apiCalls."))
                return super.loadClass(name);

        try {
        	String className = name.substring(name.lastIndexOf(".")+1);
        	String url = "file:/0/arcomemDB/ApiCalls/"+className+ ".class";
        	//String url = "jar:file:/0/arcomemDB/H2RDFApiCalls.jar!/gr/ntua/h2rdf/apiCalls/" + className+ ".class";
            URL myUrl = new URL(url);
            URLConnection connection = myUrl.openConnection();
            InputStream input = connection.getInputStream();
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            int data = input.read();

            while(data != -1){
                buffer.write(data);
                data = input.read();
            }
            input.close();

            byte[] classData = buffer.toByteArray();

            Class<?> ret = defineClass("gr.ntua.h2rdf.apiCalls."+className,
                    classData, 0, classData.length);

            
            
            return ret;
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace(); 
        }

        return null;
    }

}
