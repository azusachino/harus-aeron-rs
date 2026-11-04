import {test,expect} from '@playwright/test';
import { mkdir } from 'node:fs/promises';
test('docs navigation, assessment, theme, mobile, diagrams, and no character artwork',async({page})=>{
 const errors:string[]=[];const broken:string[]=[];page.on('pageerror',e=>errors.push(e.message));page.on('response',r=>{if(r.status()>=400)broken.push(`${r.status()} ${r.url()}`)});
 await page.goto('/');await expect(page.getByRole('heading',{name:'Aeron in Rust'})).toBeVisible();await mkdir('../../.tmp/harus-aeron-rs-astro',{recursive:true});await page.screenshot({path:'../../.tmp/harus-aeron-rs-astro/home.png',fullPage:true});
 await expect(page.getByRole('link',{name:'Tutorial'}).first()).toBeVisible();await page.getByRole('searchbox',{name:'Search documents'}).fill('1.54.0 baseline');await expect(page.getByRole('option',{name:/Aeron 1.54.0 baseline assessment/i})).toBeVisible();
 await page.goto('/tutorial/README');await expect(page.getByRole('heading',{name:/Aeron Rust Course.*Tutorial Index/})).toBeVisible();
 await page.goto('/tutorial/01-agrona-core');await expect(page.getByRole('heading',{name:/Agrona Concurrent Primitives/})).toBeVisible();
 await page.goto('/aeron-1.54.0-baseline-assessment');await expect(page.getByRole('heading',{name:/Aeron 1.54.0 baseline assessment/i})).toBeVisible();await page.screenshot({path:'../../.tmp/harus-aeron-rs-astro/assessment.png',fullPage:true});
 await page.getByRole('button',{name:/Theme/}).click();await expect(page.locator('html')).toHaveAttribute('data-theme','dark');await page.reload();await expect(page.locator('html')).toHaveAttribute('data-theme','dark');
 await page.goto('/');await expect(page.locator('svg').first()).toBeVisible();
 await page.emulateMedia({colorScheme:'dark'});await page.evaluate(()=>{localStorage.removeItem('aeron-theme');location.reload()});await expect(page.locator('html')).toHaveAttribute('data-theme','dark');
 await page.setViewportSize({width:390,height:844});await expect(page.locator('body')).toHaveJSProperty('scrollWidth',390);
 await expect(page.locator('img')).toHaveCount(0);expect(errors).toEqual([]);expect(broken).toEqual([]);
});
