package com.cdcex.server;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import org.springframework.web.filter.OncePerRequestFilter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public final class ApiAuthFilter extends OncePerRequestFilter {
    private final byte[] token;
    public ApiAuthFilter(String token){if(token==null||token.length()<32||token.length()>4096)throw new IllegalArgumentException("Admin token must contain 32 to 4096 characters");this.token=token.getBytes(StandardCharsets.UTF_8);}
    @Override protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain)throws ServletException,IOException{
        response.setHeader("X-Content-Type-Options","nosniff");response.setHeader("X-Frame-Options","DENY");response.setHeader("Referrer-Policy","no-referrer");
        response.setHeader("Content-Security-Policy","default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'");
        String path=request.getRequestURI();
        boolean read=request.getMethod().equals("GET")||request.getMethod().equals("HEAD");
        boolean publicResource=read&&(path.equals("/")||path.equals("/index.html")||path.equals("/favicon.svg")||path.equals("/api/health")||path.matches("/assets/[A-Za-z0-9_.-]+"));
        if(!publicResource||path.startsWith("/api/")){
            response.setHeader("Cache-Control","no-store");
            if(!publicResource){
                String header=request.getHeader("Authorization");
                if(header==null||!header.startsWith("Bearer ")||!MessageDigest.isEqual(token,header.substring(7).getBytes(StandardCharsets.UTF_8))){
                    response.setStatus(401);response.setContentType("application/json");response.getWriter().write("{\"code\":\"UNAUTHORIZED\",\"message\":\"A valid admin token is required\"}");return;
                }
            }
            if(request.getContentLengthLong()>1048576){response.sendError(413);return;}
        }
        chain.doFilter(new HttpServletRequestWrapper(request){
            private ServletInputStream bounded;
            @Override public ServletInputStream getInputStream()throws IOException{
                if(bounded!=null)return bounded;ServletInputStream input=super.getInputStream();
                bounded=new ServletInputStream(){
                    long count;
                    private void add(int n)throws IOException{if(n>0&&(count+=n)>1048576)throw new IOException("Request body limit exceeded");}
                    @Override public int read()throws IOException{int b=input.read();if(b>=0)add(1);return b;}
                    @Override public int read(byte[] b,int offset,int length)throws IOException{int n=input.read(b,offset,(int)Math.min(length,Math.max(1,1048577-count)));add(n);return n;}
                    @Override public boolean isFinished(){return input.isFinished();}
                    @Override public boolean isReady(){return input.isReady();}
                    @Override public void setReadListener(ReadListener listener){input.setReadListener(listener);}
                    @Override public void close()throws IOException{input.close();}
                };return bounded;
            }
            @Override public BufferedReader getReader()throws IOException{return new BufferedReader(new InputStreamReader(getInputStream(),StandardCharsets.UTF_8));}
        },response);
    }
}
